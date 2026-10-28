#!/usr/bin/env python3
"""Paced RIPE Atlas firing, shared by the target check and the seven-day rounds.

WHY PACING IS NEEDED, measured on 26 Oct 2026 (1_target_check)
  Firing 2,665 one-off traceroutes as fast as the API accepted them failed in two ways.
  The account refused new measurements once 100 were open ("You are not permitted to run
  more than 100 concurrent measurements"). And a probe works through one user's queue one
  traceroute at a time, about one every 2 seconds: the first result for queue position 0
  came 8 s after creation, for position 350 after 717 s. One-offs left waiting about ten
  minutes were closed as Failed, most with no results, and those created after the refusal
  stayed Scheduled and never ran.

THE RULE HERE
  Keep at most WINDOW of our measurements open, and only open a new one when an earlier one
  has all its results or is final. Count every open measurement on the account, ours or
  anyone else's, and keep the total under CAP_MARGIN, well below the 100 cap. A measurement
  open longer than GIVE_UP_S is abandoned and reported, never waited on forever.

  STOP WHAT IS FINISHED (27 Oct 2026). Atlas keeps a one-off counted as open for several
  minutes after its last result. The re-check of 26 Oct spent most of its time paused at the
  account margin for that reason alone: 1,084 of 1,085 measurements had both results, yet
  the account count stayed near 80. So a measurement is stopped as soon as it closes, which
  takes it off the account count at once and costs nothing, since its results are in.

  WAIT ONLY FOR PROBES THAT ARE CONNECTED. A measurement asks for every listed probe, and one
  disconnected probe would hold every measurement open until GIVE_UP_S. `connected()` is
  checked when firing starts, and a measurement closes once every connected probe has
  answered, or after STRAGGLER_S with at least one answer.

  A network error never ends a run: the loop waits and retries.

The key is read from RIPE_API_KEY in pkinternet/.env and never printed or written.
"""
import concurrent.futures
import io
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = os.path.normpath(os.path.join(HERE, "..", "..", "..", "pkinternet", ".env"))
WINDOW = 30            # our open measurements at once
CAP_MARGIN = 80        # every open measurement on the account, against the cap of 100
GIVE_UP_S = 900        # abandon a measurement open this long with no result
STRAGGLER_S = 240      # stop waiting on one open this long that has at least one result
                       # (28 Oct: with Falcon offline, waiting 420 s on every measurement
                       # halved a round's pace; the probes' results normally land within ~70 s)
POLL_S = 15
FINAL = {"Stopped", "Failed", "No suitable probes", "Denied", "Forced to stop", "Archived"}
_ctx = ssl.create_default_context()
_key = None


def key():
    """RIPE_API_KEY from the environment (a runner elsewhere), else from pkinternet/.env."""
    global _key
    if _key is None:
        _key = os.environ.get("RIPE_API_KEY")
        if not _key and os.path.exists(ENV):
            for line in io.open(ENV, encoding="utf-8", errors="replace"):
                m = re.match(r'\s*RIPE_API_KEY\s*=\s*(.+)', line)
                if m:
                    _key = m.group(1).strip().strip('"\'')
        if not _key:
            sys.exit("no RIPE_API_KEY in the environment or in pkinternet/.env")
    return _key


def api(path, payload=None, method=None, **params):
    url = "https://atlas.ripe.net/api/v2/" + path
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, data=json.dumps(payload).encode() if payload else None,
                                 headers={"Content-Type": "application/json", "Authorization": "Key " + key()},
                                 method=method)
    with urllib.request.urlopen(req, timeout=120, context=_ctx) as r:
        body = r.read()
    return json.loads(body) if body else None


def account_open():
    """Every measurement on the account that is specified, scheduled or ongoing."""
    return api("measurements/my/", status__in="0,1,2", page_size=1, fields="id")["count"]


def results(mid):
    try:
        return api("measurements/%d/results/" % mid)
    except Exception:
        return None


def statuses(ids):
    """Status of each measurement, one API call per 100.

    Completion is judged from downloaded results, never from participant_count: that counts
    the probes Atlas ASSIGNED, not the results returned. On 27 Oct three measurements showed 8
    of 8 after 18 s while only 15 of their 24 results existed."""
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 100):
        d = api("measurements/my/", id__in=",".join(map(str, ids[i:i + 100])), page_size=100,
                fields="id,status")
        for r in d["results"]:
            out[r["id"]] = r["status"]["name"]
    return out


def stop(ids, threads=8):
    """Stop our own measurements. Returns how many the API accepted."""
    def one(m):
        try:
            api("measurements/%d/" % m, method="DELETE")
            return 1
        except urllib.error.HTTPError:
            return 0
    with concurrent.futures.ThreadPoolExecutor(threads) as ex:
        return sum(ex.map(one, ids))


def connected(probe_ids):
    """The listed probes Atlas reports as Connected right now, with what each says about itself."""
    out = {}
    for p in probe_ids:
        d = api("probes/%d/" % p)
        if d["status"]["name"] == "Connected":
            out[p] = dict(address_v4=d.get("address_v4"), asn_v4=d.get("asn_v4"),
                          prefix_v4=d.get("prefix_v4"), country=d.get("country_code"))
    return out


def fire_paced(definitions, probe_ids, on_created, on_done, log=print, window=WINDOW, stop_closed=True,
               refresh=None, refresh_s=600, prune=None, prune_s=60):
    """Create one-off measurements at the pace the probes can absorb.

    definitions   list of (tag, definition dict); one measurement each
    probe_ids     the probes to ask; pass only connected ones (see connected())
    on_created    called (tag, measurement id) as soon as a measurement exists, to checkpoint
    on_done       called (tag, measurement id, results list, why) when a measurement closes
    refresh       optional; called every refresh_s, returns the probes connected now. New
                  measurements then ask only those, so a probe that drops out mid-run stops
                  being waited on. Each measurement keeps the probe count it was created with.
    prune         optional; called every prune_s, returns tags that already have a measurement
                  (made by anyone). Those leave the queue, so two runners that started together
                  stop duplicating each other once Atlas's listing catches up (about a minute).
    """
    pending = list(definitions)
    open_ = {}                                  # id -> (tag, opened at, probes asked)
    probe_ids = sorted(probe_ids)
    last_refresh = last_prune = time.time()
    t0 = time.time()
    while pending or open_:
        try:
            if prune and pending and time.time() - last_prune > prune_s:
                last_prune = time.time()
                taken = prune()
                before = len(pending)
                pending = [(t, d) for t, d in pending if t not in taken]
                if len(pending) < before:
                    log("  %d queued targets already have a measurement from another runner; dropped"
                        % (before - len(pending)))
            if refresh and pending and time.time() - last_refresh > refresh_s:
                last_refresh = time.time()
                now_up = sorted(refresh())
                if now_up and now_up != probe_ids:
                    log("  probes connected now %s (were %s)" % (now_up, probe_ids))
                    probe_ids = now_up
            source = [dict(type="probes", value=",".join(map(str, probe_ids)), requested=len(probe_ids))]
            if open_:
                ids = list(open_)
                with concurrent.futures.ThreadPoolExecutor(12) as ex:
                    got = dict(zip(ids, ex.map(results, ids)))
                st = statuses(ids)
                answered = {m: len(got.get(m) or []) for m in ids}
                closed = []
                for m in ids:
                    tag, opened, want = open_[m]
                    n_ans = answered.get(m, 0)
                    age = time.time() - opened
                    why = ("complete" if n_ans >= want else
                           "final: " + st[m] if st.get(m) in FINAL else
                           "straggler after %d s, %d of %d" % (STRAGGLER_S, n_ans, want)
                           if n_ans and age > STRAGGLER_S else
                           "abandoned after %d s" % GIVE_UP_S if age > GIVE_UP_S else None)
                    if why:
                        on_done(tag, m, got.get(m) or [], why)
                        del open_[m]
                        # stop only what is complete: a straggler left open can still deliver
                        # its late probe's result, and Atlas closes it on its own
                        if why == "complete" and st.get(m) not in FINAL:
                            closed.append(m)
                if closed and stop_closed:
                    stop(closed)
            if pending:
                room = min(window - len(open_), CAP_MARGIN - account_open())
                if room > 0:
                    batch, pending = pending[:min(room, 50)], pending[min(room, 50):]
                    payload = dict(definitions=[d for _, d in batch], is_oneoff=True, probes=source)
                    try:
                        created = api("measurements/", payload)["measurements"]
                        for (tag, _), m in zip(batch, created):
                            open_[m] = (tag, time.time(), len(probe_ids))
                            on_created(tag, m)
                    except urllib.error.HTTPError as e:
                        log("  create refused, HTTP %d %s; will retry" % (e.code, e.read().decode(errors="replace")[:160]))
                        pending = batch + pending
        except (urllib.error.URLError, OSError, ValueError) as e:
            # network blip or a truncated reply: nothing is lost, the next pass retries
            log("  network error (%s); retrying" % type(e).__name__)
        log("  %5.1f min  open %3d  pending %5d" % ((time.time() - t0) / 60, len(open_), len(pending)))
        time.sleep(POLL_S)
