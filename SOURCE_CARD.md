# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:**
**Date:** Sat 14 November 2026

## 1. The source
- Source and URL: Bluesky Jetstream, `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Method (WebSocket push, REST polling or REST history pull): WebSocket push 
- Account, key or User-Agent needed: none
- Limits I know about (rate limits, paging, reconnects): no rate limit

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:jfszh4p7zwvlmp727c4ipods",
  "time_us": 1794688578453628,
  "cursor": 27190444941,
  "kind": "commit",
  "commit": {
    "rev": "3n2cdzmzek526",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n2cdzmyqz526",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-14T20:36:16.389064+00:00",
      "langs": [
        "en"
      ],
      "text": "📉 Smart Money DUMPED $16,617 of #NEAR\n💵 Price: $3.024000\nSize: 5.50K NEAR\n📈 Trend: 15m -0.79% | 1h -2.04%\n\n#Crypto #NEAR #AI #DePIN"
    },
    "cid": "bafyreiejuc7sqf7xuy7lg5m3w5auptaxagymgd66dxzzbt3pczkudthdkm"
  }
}
```

## 3. Volume maths
- Measured rate (from the probe): 42.38 per second (messages: 2544   not JSON: 0)
- Live capture needed for 50,000: about 1,179.9 seconds (20 min)
- History / backfill available? How many days? Not needed, live capture reaches 50,000 in about 20 minutes. 
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): run producer for about 45 minutes, which gives about 100,000 records (2x margin).

## 4. Topic design
- Topic name: bluesky_posts        (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: did
- Why this key (ordering, spread across partitions): All posts from one account go to the same partition, so they stay in order.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) The same partition and offset. This happens if the lander writes a file and crashes before storing offsets, so it reads the same records again.
- What makes two records **the same thing** (the same trade, post, aircraft position)? The same did and commit.rkey, so one account's one post.
- If the same thing can arrive in several versions, which one do I keep? The latest version.

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: What percentage of new Bluesky posts are replies (opposed to original posts), and how does that share differ between the top six languages? 

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join