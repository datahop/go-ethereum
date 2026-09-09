#!/usr/bin/env python3
"""Poll every node's /status and summarise connections; N nodes, every I seconds, R rounds."""
import json, sys, time, urllib.request, statistics as st
N=int(sys.argv[1]); I=int(sys.argv[2]); R=int(sys.argv[3])
prev=None
for r in range(R):
    rows=[]
    for i in range(N):
        try: rows.append(json.load(urllib.request.urlopen(f"http://127.0.0.1:{40300+i}/status", timeout=2)))
        except Exception: rows.append(None)
    up=[x for x in rows if x]
    out=[x["outbound"] for x in up]; inn=[x["inbound"] for x in up]; peers=[x["peers"] for x in up]
    full=sum(1 for o in out if o>=16); tab=[x["table"] for x in up]; ads=[x["ads_held"] for x in up]
    print(f"t={r*I:4d}s up={len(up)}/{N} peers p50={st.median(peers) if peers else 0:.0f} min={min(peers,default=0)} | out p50={st.median(out) if out else 0:.0f} full(16)={full} | in p50={st.median(inn) if inn else 0:.0f} max={max(inn,default=0)} | table p50={st.median(tab) if tab else 0:.0f} | ads p50={st.median(ads) if ads else 0:.0f} total={sum(ads)}", flush=True)
    if r<R-1: time.sleep(I)
