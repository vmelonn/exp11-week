#!/usr/bin/env python3
"""11.1 step 2 - the seven daily rounds, run so that no machine has to stay on.

ATLAS IS THE STATE. Every measurement is created with the description
"exp11 week D<n> <target>". A runner works out which day it is from week.json, asks Atlas
which of that day's targets already have a measurement, and fires only the rest, paced by
../atlas_pacing.py. Nothing needed to resume lives on the machine that fired: a runner killed
by a shutdown loses at most the measurements it had in flight, and those finish on Atlas
anyway. The next runner, on the same machine or another, carries on. Results stay on Atlas
and `fetch` downloads them whenever a machine is on.

THE DAY. Day n is the 24 hours from start_utc + (n - 1) days. A runner started anywhere in
that window completes day n; a day whose window passes with targets unfired is reported as
partial, never back-filled under its own tag, since its traces would carry the wrong date.
Every result carries its own timestamp, so a late round is visible in the data.

T1, same vantage every round, needs no stored state either: each Atlas result records the
probe's public address ("from") and its source address, and `fetch` compares them across days.

A TARGET IS DONE FOR THE DAY when one of its measurements that day is still open or has at
least one result. One that closed empty is fired again, at most MAX_TRIES times a day.

    python -I run_week.py dryrun      schedule, panel, connected probes, cost. Fires nothing
    python -I run_week.py round       fire what today's round still lacks. SPENDS CREDITS
    python -I run_week.py status      per day: targets fired, open, empty
    python -I run_week.py fetch       download every day's results, write routes_D<n>.txt

The key is RIPE_API_KEY from the environment, else from pkinternet/.env. It is never printed
or written.
"""
import collections
import concurrent.futures
import datetime
import gzip
import io
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.dirname(HERE)]       # atlas_pacing beside us, or one level up
import atlas_pacing as ap  # noqa: E402

CONFIG = os.environ.get("WEEK_CONFIG") or os.path.join(HERE, "week.json")   # override only for a test
ROUNDS = None         # local output folder, from week.json
TAG = None            # "<tag> D%d ", from week.json
MAX_TRIES = 2
LOCK_S = 600          # a measurement of today created this recently means another runner is active
LOCK_STALE_S = 300    # the local lock is touched every poll (15 s); older than this, its runner is gone
SETTLE_S = 120        # Atlas's filtered listing lags creation; wait this long before firing
CREDITS_PER_RESULT = 60


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def load(path):
    with io.open(path, encoding="utf-8") as fh:
        return json.load(fh)


def config():
    global TAG, ROUNDS
    c = load(CONFIG)
    TAG = c["tag"] + " D%d "
    ROUNDS = os.path.join(HERE, c["rounds_dir"])
    if not c.get("start_utc"):
        sys.exit("week.json has no start_utc yet; it is set when the panel is frozen. Nothing fired.")
    if not os.path.exists(os.path.join(HERE, c["panel"])):
        sys.exit("no panel at %s; freeze it first (1_target_check: target_check.py panel)" % c["panel"])
    c["start"] = datetime.datetime.strptime(c["start_utc"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=datetime.timezone.utc)
    c["targets"] = [t["target"] for t in load(os.path.join(HERE, c["panel"]))["targets"]]
    return c


def day_of(c, t=None):
    d = int(((t or now()) - c["start"]).total_seconds() // 86400) + 1
    return d if 1 <= d <= c["days"] else None


def listing(d):
    """Every measurement of ours tagged for day d: id, target, status, created, from Atlas."""
    out, page = [], ap.api("measurements/my/", description__startswith=TAG % d, page_size=500,
                          fields="id,description,status,creation_time,participant_count")
    while True:
        out += page["results"]
        if not page.get("next"):
            return [dict(id=r["id"], target=r["description"][len(TAG % d):], status=r["status"]["name"],
                         created=r["creation_time"], answered=r.get("participant_count") or 0) for r in out]
        page = ap.api(page["next"].split("/api/v2/")[1])


OPEN = ("Specified", "Scheduled", "Ongoing")


def done_and_tries(d):
    """Targets already settled for day d, measurements per target, and the newest creation time.

    Settled: a measurement that day has an answer from at least one probe (participant_count,
    which matches the result count), or is still open and younger than atlas_pacing.GIVE_UP_S,
    so another runner may yet collect it. Needs one listing call, no result downloads."""
    ms = listing(d)
    t = now().timestamp()
    done, tries = set(), collections.Counter()
    for m in ms:
        tries[m["target"]] += 1
        if m["answered"] > 0 or (m["status"] in OPEN and t - m["created"] < ap.GIVE_UP_S):
            done.add(m["target"])
    newest = max((m["created"] for m in ms), default=0)
    return done, tries, newest


def keep_awake():
    """Ask Windows not to sleep while a round runs. A shutdown still ends it; nothing is lost."""
    if os.name == "nt":
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)


def definition(c, d, t):
    return dict(target=t, af=4, type="traceroute", protocol=c["protocol"], packets=c["packets"],
                paris=c["paris"], resolve_on_probe=False, is_oneoff=True, description=(TAG % d) + t)


def dryrun():
    c = config()
    up = ap.connected(c["probes"])
    per_round = len(c["targets"]) * len(c["probes"]) * CREDITS_PER_RESULT
    print("start %s UTC, %d days; today is %s" % (c["start_utc"], c["days"], day_of(c) or "outside the week"))
    for n in range(1, c["days"] + 1):
        s = c["start"] + datetime.timedelta(days=n - 1)
        print("  D%d  %s to %s UTC" % (n, s.strftime("%a %d %b %H:%M"), (s + datetime.timedelta(days=1)).strftime("%a %d %b %H:%M")))
    print("panel %d targets; probes %s" % (len(c["targets"]), c["probes"]))
    print("connected now: %s%s" % (sorted(up), "" if len(up) == len(c["probes"]) else
                                   "   NOT connected: %s" % sorted(set(c["probes"]) - set(up))))
    print("cost at most %d credits a round, %d for the week; balance %d. Nothing fired."
          % (per_round, per_round * c["days"], ap.api("credits/")["current_balance"]))


def round_():
    c = config()
    d = day_of(c)
    if d is None:
        print("outside the week (%s UTC); nothing to do" % now().strftime("%Y-%m-%d %H:%M"))
        return
    os.makedirs(os.path.join(ROUNDS, "D%d" % d), exist_ok=True)
    lock = os.path.join(ROUNDS, "runner.lock")

    # Guard 1, same machine: a lock file touched at every poll. Stale after LOCK_STALE_S.
    if os.path.exists(lock) and now().timestamp() - os.path.getmtime(lock) < LOCK_STALE_S:
        print("another runner on this machine is active (lock touched %d s ago); exiting"
              % (now().timestamp() - os.path.getmtime(lock)))
        return
    io.open(lock, "w").write(str(os.getpid()))
    log_path = os.path.join(ROUNDS, "D%d" % d, "run_%s.log" % now().strftime("%Y%m%dT%H%MZ"))
    logf = io.open(log_path, "w", encoding="utf-8")

    def log(s):
        print(s, flush=True)
        logf.write(s + "\n")
        logf.flush()
        io.open(lock, "w").write(str(os.getpid()))      # heartbeat: the lock stays fresh while we run

    try:
        _round_body(c, d, log)
    finally:
        if os.path.exists(lock):
            os.remove(lock)
        logf.close()


def _round_body(c, d, log):
    def left(done_, tries_):
        return [t for t in c["targets"] if t not in done_ and tries_[t] < MAX_TRIES]

    done, tries, newest = done_and_tries(d)
    if left(done, tries):
        # Guard 2, another machine. Atlas's filtered listing shows a new measurement only after a
        # lag (two runners 30 s apart both saw nothing on 27 Oct), so wait it out and look again.
        # Then, if today's tag got a measurement in the last LOCK_S, a runner may be active: leave
        # it the day. The cost of being wrong is one trigger's delay; the cost of the opposite
        # mistake is double-firing.
        log("D%d: work to do; waiting %d s for Atlas's listing to catch up" % (d, SETTLE_S))
        time.sleep(SETTLE_S)
        done, tries, newest = done_and_tries(d)
        if left(done, tries) and now().timestamp() - newest < LOCK_S:
            log("D%d: a measurement was created %d s ago; a runner may be active elsewhere. Exiting; "
                "the next trigger carries on." % (d, now().timestamp() - newest))
            return
    todo = [t for t in c["targets"] if t not in done and tries[t] < MAX_TRIES]
    gave_up = [t for t in c["targets"] if t not in done and tries[t] >= MAX_TRIES]
    log("D%d (%s UTC): %d of %d targets settled, %d to fire, %d given up after %d tries"
        % (d, now().strftime("%Y-%m-%d %H:%M"), len(done), len(c["targets"]), len(todo), len(gave_up), MAX_TRIES))
    if not todo:
        return
    up = ap.connected(c["probes"])
    if len(up) < c["min_probes"]:
        log("only %d probes connected (%s), fewer than %d; not firing" % (len(up), sorted(up), c["min_probes"]))
        return
    cost = len(todo) * len(up) * CREDITS_PER_RESULT
    bal = ap.api("credits/")["current_balance"]
    log("probes connected %s; at most %d credits; balance %d" % (sorted(up), cost, bal))
    if bal < cost:
        log("insufficient credits; not firing")
        return
    json.dump(dict(day=d, at=now().strftime("%Y-%m-%dT%H:%M:%SZ"), probes=up),
              io.open(os.path.join(ROUNDS, "D%d" % d, "probes_%s.json" % now().strftime("%Y%m%dT%H%MZ")),
                      "w", encoding="utf-8"), indent=1)
    keep_awake()
    why = collections.Counter()
    end = c["start"] + datetime.timedelta(days=d)

    def on_done(tag, m, res, w):
        why[w.split(":")[0].split(" after")[0]] += 1

    # fire in slices so a round never spills into the next day's window
    while todo and now() < end:
        slice_, todo = todo[:500], todo[500:]
        ap.fire_paced([(t, definition(c, d, t)) for t in slice_], sorted(up), lambda t, m: None, on_done,
                      log=log, window=c["window"])
    log("D%d run closed: %s; %d targets not reached before the day ended" % (d, dict(why), len(todo)))


def status():
    c = config()
    print("today: %s" % (("D%d" % day_of(c)) if day_of(c) else "outside the week"))
    for d in range(1, c["days"] + 1):
        ms = listing(d)
        if not ms:
            continue
        st = collections.Counter(m["status"] for m in ms)
        print("  D%d  measurements %d, targets %d of %d, by status %s"
              % (d, len(ms), len({m["target"] for m in ms}), len(c["targets"]), dict(st)))


def hops(r):
    res = []
    for h in r.get("result", []):
        if h.get("hop") == 255:
            continue
        pk = h.get("result", [])
        ip = next((x.get("from") for x in pk if x.get("from")), None)
        rt = [x["rtt"] for x in pk if isinstance(x.get("rtt"), (int, float))]
        res.append((h.get("hop"), ip, min(rt) if rt else None))
    return res


def fetch():
    """Download every day's results; write raw_D<n>.json.gz, routes_D<n>.txt and the T1 table."""
    c = config()
    t1 = collections.defaultdict(dict)
    for d in range(1, c["days"] + 1):
        ms = listing(d)
        if not ms:
            continue
        with concurrent.futures.ThreadPoolExecutor(12) as ex:
            got = dict(zip([m["id"] for m in ms], ex.map(lambda m: ap.results(m["id"]) or [], ms)))
        out_dir = os.path.join(ROUNDS, "D%d" % d)
        os.makedirs(out_dir, exist_ok=True)
        with gzip.open(os.path.join(out_dir, "raw_D%d.json.gz" % d), "wt", encoding="utf-8") as fh:
            json.dump(dict(day=d, measurements=ms, results={str(k): v for k, v in got.items()}), fh)
        lines = ["%s D%d. %s, paris=%d, min of %d packets per hop. '*' = no reply."
                 % (c["tag"], d, c["protocol"], c["paris"], c["packets"]), ""]
        seen = collections.defaultdict(lambda: collections.Counter())
        for m in ms:
            for r in sorted(got[m["id"]], key=lambda x: x.get("prb_id") or 0):
                hs = hops(r)
                seen[r["prb_id"]][(r.get("from"), r.get("src_addr"))] += 1
                lines.append("D%d %s UTC probe %s (from %s) -> %s  reached=%s" % (
                    d, datetime.datetime.fromtimestamp(r["timestamp"], datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    r.get("prb_id"), r.get("from"), r.get("dst_addr"), any(ip == r.get("dst_addr") for _, ip, _ in hs)))
                for hn, ip, rt in hs:
                    lines.append("   %3s  %-17s %s" % (hn, ip or "*", ("%.1f ms" % rt) if rt is not None else ""))
                lines.append("")
        io.open(os.path.join(out_dir, "routes_D%d.txt" % d), "w", encoding="utf-8").write("\n".join(lines))
        for p, cnt in seen.items():
            t1[str(p)]["D%d" % d] = {"%s / %s" % k: v for k, v in cnt.most_common()}
        n = sum(len(v) for v in got.values())
        print("D%d: %d measurements, %d results, written to %s" % (d, len(ms), n, os.path.relpath(out_dir, HERE)))
    json.dump(t1, io.open(os.path.join(ROUNDS, "t1_vantage_by_day.json"), "w", encoding="utf-8"), indent=1)
    for p, days in sorted(t1.items()):
        addrs = {k for v in days.values() for k in v}
        print("  T1 probe %s: %s" % (p, "same public / source address every day" if len(addrs) == 1
                                     else "CHANGED: %s" % {d: list(v) for d, v in days.items()}))


if __name__ == "__main__":
    step = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    dict(dryrun=dryrun, round=round_, status=status, fetch=fetch)[step]()
