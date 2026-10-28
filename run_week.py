#!/usr/bin/env python3
"""11.1 step 2 - the week's rounds, run so that no machine has to stay on.

ROUNDS. week.json sets the start, the number of days and rounds_per_day. With two a day
(decided 27 Oct 2026, for a day/night comparison) round R1 starts at start_utc, R2 twelve
hours later, and so on. The week starts 27 Oct at 22:00 Pakistan time, so odd rounds are the
night rounds, 22:05, and even rounds the day rounds, 10:05; labels come from the clock. Round n owns the slot from start_utc + (n - 1) x 24 h / rounds_per_day until the
next round begins. A runner started anywhere in that slot completes round n; a slot that ends
with targets unfired leaves its round partial, never back-filled under its own tag, since its
traces would carry the wrong time. Every result carries its own timestamp.

ATLAS IS THE STATE. Every measurement is created with the description
"exp11 week R<n> <target>". A runner works out the round from week.json, asks Atlas which of
that round's targets already have a measurement, and fires only the rest, paced by
../atlas_pacing.py. Nothing needed to resume lives on the machine that fired: a runner killed
mid-round loses at most the measurements it had in flight, and those finish on Atlas anyway.
The next runner, on the same machine or another, carries on. Results stay on Atlas and
`fetch` downloads them whenever a machine is on.

T1, same vantage every round, needs no stored state either: each Atlas result records the
probe's public address ("from") and its source address, and `fetch` compares them across rounds.

A TARGET IS DONE FOR THE ROUND when one of its measurements that round has an answer from a
probe, or is still open and young. One that closed empty is fired again, at most MAX_TRIES times.

    python -I run_week.py dryrun      schedule, panel, connected probes, cost. Fires nothing
    python -I run_week.py round       fire what the current round still lacks. SPENDS CREDITS
    python -I run_week.py status      per round: measurements, targets, by status
    python -I run_week.py fetch       download every round's results, write routes_R<n>.txt

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
TAG = None            # "<tag> R%d ", from week.json
MAX_TRIES = 2
LOCK_S = 600          # a measurement of this round created this recently means another runner is active
LOCK_STALE_S = 300    # the local lock is touched every poll (15 s); older than this, its runner is gone
SETTLE_S = 120        # Atlas's filtered listing lags creation; wait this long before firing
CREDITS_PER_RESULT = 60
# RIPE Atlas quotas, https://atlas.ripe.net/docs/getting-started/user-defined-measurements.html
# (read 27 Oct 2026): 100 simultaneous measurements (kept under 80 by atlas_pacing), 100,000
# results a day, 1,000,000 credits a day by default (this account's ceiling, from the credits
# API, is 10,000,000), 25 one-off measurements of one type against one target at a time (we
# never exceed MAX_TRIES = 2). A round is refused if it would take the last 24 hours past
# LIMIT_SHARE of the daily results or credits.
RESULTS_PER_DAY = 100000
LIMIT_SHARE = 0.9
PKT = datetime.timezone(datetime.timedelta(hours=5), "PKT")   # Pakistan, no daylight saving


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def load(path):
    with io.open(path, encoding="utf-8") as fh:
        return json.load(fh)


def config():
    global TAG, ROUNDS
    c = load(CONFIG)
    TAG = c["tag"] + " R%d "
    ROUNDS = os.path.join(HERE, c["rounds_dir"])
    if not c.get("start_utc"):
        sys.exit("week.json has no start_utc yet; it is set when the panel is frozen. Nothing fired.")
    if not os.path.exists(os.path.join(HERE, c["panel"])):
        sys.exit("no panel at %s; freeze it first (1_target_check: target_check.py panel)" % c["panel"])
    c["start"] = datetime.datetime.strptime(c["start_utc"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=datetime.timezone.utc)
    c["slot"] = datetime.timedelta(days=1) / c.get("rounds_per_day", 1)
    c["rounds"] = c["days"] * c.get("rounds_per_day", 1)
    c["targets"] = [t["target"] for t in load(os.path.join(HERE, c["panel"]))["targets"]]
    return c


def round_of(c, t=None):
    n = int(((t or now()) - c["start"]) // c["slot"]) + 1
    return n if 1 <= n <= c["rounds"] else None


def listing(n):
    """Every measurement of ours tagged for round n: id, target, status, created, answered."""
    out, page = [], ap.api("measurements/my/", description__startswith=TAG % n, page_size=500,
                          fields="id,description,status,creation_time,participant_count")
    while True:
        out += page["results"]
        if not page.get("next"):
            return [dict(id=r["id"], target=r["description"][len(TAG % n):], status=r["status"]["name"],
                         created=r["creation_time"], answered=r.get("participant_count") or 0) for r in out]
        page = ap.api(page["next"].split("/api/v2/")[1])


OPEN = ("Specified", "Scheduled", "Ongoing")


def done_and_tries(n):
    """Targets already settled for round n, measurements per target, and the newest creation time.

    Settled: a measurement that round was assigned to at least one probe (participant_count,
    which counts ASSIGNED probes, not returned results), or is still open and younger than
    atlas_pacing.GIVE_UP_S. One listing call, no result downloads, which keeps the hourly checks
    cheap. The cost: a measurement that was assigned but returned nothing is not re-fired; it
    shows as empty in `fetch`."""
    ms = listing(n)
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


def definition(c, n, t):
    return dict(target=t, af=4, type="traceroute", protocol=c["protocol"], packets=c["packets"],
                paris=c["paris"], resolve_on_probe=False, is_oneoff=True, description=(TAG % n) + t)


def when(c, n):
    s = c["start"] + (n - 1) * c["slot"]
    h = s.astimezone(PKT).hour
    kind = ("day" if 6 <= h < 18 else "night") if c.get("rounds_per_day", 1) > 1 else ""
    return "R%-2d %-5s %s UTC, %s Pakistan" % (n, kind, s.strftime("%a %d %b %H:%M"),
                                               s.astimezone(PKT).strftime("%a %H:%M"))


def dryrun():
    c = config()
    up = ap.connected(c["probes"])
    per_round = len(c["targets"]) * len(c["probes"]) * CREDITS_PER_RESULT
    cur = round_of(c)
    print("start %s UTC, %d days, %d rounds a day; now is %s"
          % (c["start_utc"], c["days"], c.get("rounds_per_day", 1), ("R%d" % cur) if cur else "outside the week"))
    for n in range(1, c["rounds"] + 1):
        print("  " + when(c, n))
    print("panel %d targets; probes %s" % (len(c["targets"]), c["probes"]))
    print("connected now: %s%s" % (sorted(up), "" if len(up) == len(c["probes"]) else
                                   "   NOT connected: %s" % sorted(set(c["probes"]) - set(up))))
    cr = ap.api("credits/")
    per_day = per_round * c.get("rounds_per_day", 1)
    print("cost at most %d credits a round; %d A DAY against the account's daily ceiling of %d (%s); "
          "%d for the week" % (per_round, per_day, cr["max_daily_credits"],
                               "fits" if per_day < cr["max_daily_credits"] else "OVER THE CEILING",
                               per_round * c["rounds"]))
    print("results at most %d a day against Atlas's documented 100,000; balance %d. Nothing fired."
          % (len(c["targets"]) * len(c["probes"]) * c.get("rounds_per_day", 1), cr["current_balance"]))


def round_():
    c = config()
    n = round_of(c)
    if n is None:
        print("outside the week (%s UTC); nothing to do" % now().strftime("%Y-%m-%d %H:%M"))
        return
    os.makedirs(os.path.join(ROUNDS, "R%d" % n), exist_ok=True)
    lock = os.path.join(ROUNDS, "runner.lock")

    # Guard 1, same machine: a lock file touched at every poll. Stale after LOCK_STALE_S.
    if os.path.exists(lock) and now().timestamp() - os.path.getmtime(lock) < LOCK_STALE_S:
        print("another runner on this machine is active (lock touched %d s ago); exiting"
              % (now().timestamp() - os.path.getmtime(lock)))
        return
    io.open(lock, "w").write(str(os.getpid()))
    log_path = os.path.join(ROUNDS, "R%d" % n, "run_%s.log" % now().strftime("%Y%m%dT%H%MZ"))
    logf = io.open(log_path, "w", encoding="utf-8")

    def log(s):
        print(s, flush=True)
        logf.write(s + "\n")
        logf.flush()
        io.open(lock, "w").write(str(os.getpid()))      # heartbeat: the lock stays fresh while we run

    try:
        _round_body(c, n, log)
    finally:
        if os.path.exists(lock):
            os.remove(lock)
        logf.close()


def _round_body(c, n, log):
    def left(done_, tries_):
        return [t for t in c["targets"] if t not in done_ and tries_[t] < MAX_TRIES]

    done, tries, newest = done_and_tries(n)
    if left(done, tries):
        # Guard 2, another machine. Atlas's filtered listing shows a new measurement only after a
        # lag (two runners 9 s apart both saw nothing on 27 Oct), so wait it out and look again.
        # Then, if this round's tag got a measurement in the last LOCK_S, a runner may be active:
        # leave it the round. The cost of being wrong is one trigger's delay; the cost of the
        # opposite mistake is double-firing.
        log("R%d: work to do; waiting %d s for Atlas's listing to catch up" % (n, SETTLE_S))
        time.sleep(SETTLE_S)
        done, tries, newest = done_and_tries(n)
        if left(done, tries) and now().timestamp() - newest < c.get("lock_s", LOCK_S):
            log("R%d: a measurement was created %d s ago; a runner may be active elsewhere. Exiting; "
                "the next trigger carries on." % (n, now().timestamp() - newest))
            return
    todo = left(done, tries)
    gave_up = [t for t in c["targets"] if t not in done and tries[t] >= MAX_TRIES]
    log("%s: %d of %d targets settled, %d to fire, %d given up after %d tries (now %s UTC)"
        % (when(c, n), len(done), len(c["targets"]), len(todo), len(gave_up), MAX_TRIES,
           now().strftime("%Y-%m-%d %H:%M")))
    if not todo:
        return
    up = ap.connected(c["probes"])
    if len(up) < c["min_probes"]:
        log("only %d probes connected (%s), fewer than %d; not firing" % (len(up), sorted(up), c["min_probes"]))
        return
    cost = len(todo) * len(up) * CREDITS_PER_RESULT
    cr = ap.api("credits/")
    bal, spent, made = cr["current_balance"], cr.get("past_day_credits_spent") or 0, cr.get("past_day_measurement_results") or 0
    ceiling = cr.get("max_daily_credits") or 1000000
    log("probes connected %s; at most %d credits and %d results; balance %d; last 24 h %d credits of %d, "
        "%d results of %d" % (sorted(up), cost, len(todo) * len(up), bal, spent, ceiling, made, RESULTS_PER_DAY))
    if bal < cost:
        log("insufficient credits; not firing")
        return
    if spent + cost > LIMIT_SHARE * ceiling or made + len(todo) * len(up) > LIMIT_SHARE * RESULTS_PER_DAY:
        log("this round would pass %d%% of a daily quota (credits or results); not firing"
            % (LIMIT_SHARE * 100))
        return
    json.dump(dict(round=n, at=now().strftime("%Y-%m-%dT%H:%M:%SZ"), probes=up),
              io.open(os.path.join(ROUNDS, "R%d" % n, "probes_%s.json" % now().strftime("%Y%m%dT%H%MZ")),
                      "w", encoding="utf-8"), indent=1)
    keep_awake()
    why = collections.Counter()
    end = c["start"] + n * c["slot"]

    def on_done(tag, m, res, w):
        why[w.split(":")[0].split(" after")[0]] += 1

    # fire in slices so a round never spills into the next round's slot
    # The pacing re-checks which probes are connected every 10 minutes, so a probe that drops
    # out mid-round stops being waited on (28 Oct: Falcon went offline during R2).
    while todo and now() < end:
        slice_, todo = todo[:500], todo[500:]
        ap.fire_paced([(t, definition(c, n, t)) for t in slice_], sorted(up), lambda t, m: None, on_done,
                      log=log, window=c["window"], refresh=lambda: list(ap.connected(c["probes"])),
                      prune=lambda: done_and_tries(n)[0])
    log("R%d run closed: %s; %d targets not reached before the slot ended" % (n, dict(why), len(todo)))


def status():
    c = config()
    cur = round_of(c)
    print("now: %s" % (("R%d" % cur) if cur else "outside the week"))
    for n in range(1, c["rounds"] + 1):
        ms = listing(n)
        if not ms:
            continue
        st = collections.Counter(m["status"] for m in ms)
        print("  %s  measurements %d, targets %d of %d, by status %s"
              % (when(c, n), len(ms), len({m["target"] for m in ms}), len(c["targets"]), dict(st)))


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
    """Download every round's results; write raw_R<n>.json.gz, routes_R<n>.txt and the T1 table."""
    c = config()
    t1 = collections.defaultdict(dict)
    for n in range(1, c["rounds"] + 1):
        ms = listing(n)
        if not ms:
            continue
        with concurrent.futures.ThreadPoolExecutor(12) as ex:
            got = dict(zip([m["id"] for m in ms], ex.map(lambda m: ap.results(m["id"]) or [], ms)))
        out_dir = os.path.join(ROUNDS, "R%d" % n)
        os.makedirs(out_dir, exist_ok=True)
        with gzip.open(os.path.join(out_dir, "raw_R%d.json.gz" % n), "wt", encoding="utf-8") as fh:
            json.dump(dict(round=n, measurements=ms, results={str(k): v for k, v in got.items()}), fh)
        lines = ["%s %s. %s, paris=%d, min of %d packets per hop. '*' = no reply."
                 % (c["tag"], when(c, n), c["protocol"], c["paris"], c["packets"]), ""]
        seen = collections.defaultdict(lambda: collections.Counter())
        for m in ms:
            for r in sorted(got[m["id"]], key=lambda x: x.get("prb_id") or 0):
                hs = hops(r)
                seen[r["prb_id"]][(r.get("from"), r.get("src_addr"))] += 1
                lines.append("R%d %s UTC probe %s (from %s) -> %s  reached=%s" % (
                    n, datetime.datetime.fromtimestamp(r["timestamp"], datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    r.get("prb_id"), r.get("from"), r.get("dst_addr"), any(ip == r.get("dst_addr") for _, ip, _ in hs)))
                for hn, ip, rt in hs:
                    lines.append("   %3s  %-17s %s" % (hn, ip or "*", ("%.1f ms" % rt) if rt is not None else ""))
                lines.append("")
        io.open(os.path.join(out_dir, "routes_R%d.txt" % n), "w", encoding="utf-8").write("\n".join(lines))
        for p, cnt in seen.items():
            t1[str(p)]["R%d" % n] = {"%s / %s" % k: v for k, v in cnt.most_common()}
        k = sum(len(v) for v in got.values())
        print("R%d: %d measurements, %d results, written to %s" % (n, len(ms), k, os.path.relpath(out_dir, HERE)))
    json.dump(t1, io.open(os.path.join(ROUNDS, "t1_vantage_by_round.json"), "w", encoding="utf-8"), indent=1)
    for p, rs in sorted(t1.items()):
        addrs = {k for v in rs.values() for k in v}
        print("  T1 probe %s: %s" % (p, "same public / source address every round" if len(addrs) == 1
                                     else "CHANGED: %s" % {r: list(v) for r, v in rs.items()}))


if __name__ == "__main__":
    step = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    dict(dryrun=dryrun, round=round_, status=status, fetch=fetch)[step]()
