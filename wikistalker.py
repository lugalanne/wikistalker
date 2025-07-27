import aiohttp
import asyncio
import json
import difflib
import aiosqlite
import time
import os
import argparse
import dotenv
import logging
from rich.logging import RichHandler

logging.basicConfig(level="INFO", handlers=[RichHandler()])


parser = argparse.ArgumentParser()
parser.add_argument("--name",help="",default='')
parser.add_argument("--wikies",help="Enter target languages (en,ru,uk,de, ... etc)",type=str)
parser.add_argument("--titles",help="Enter path to article titles file (for monitoring)",type=str,default='None')
parser.add_argument("--env",help="Enter path to .env file with Telegram token",type=str,default='None')
parser.add_argument("--database",help="Enter path to database file (.db)",type=str,default='None')

args = parser.parse_args()

log = logging.getLogger('rich')

watchlist_titles = [] 
watchlist_langs = args.wikies.replace(' ','').split(',')
log.info(f'Langueges set: {watchlist_langs}')

if args.titles != 'None':
    with open(args.titles,'r',encoding='utf-8') as file:
        watchlist_titles = set(line.strip() for line in file if line.strip())

log.info(f'Monitoring articles: {watchlist_titles}')
timeout = aiohttp.ClientTimeout(total=None,sock_connect=10,sock_read=30)

async def send_to_bot(bot_token,text):
    async with aiohttp.ClientSession() as session:
       # async with session.get(f"https://api.telegram.org/bot{bot_token}/getUpdates") as resp:
        #    data=await resp.json()
        #    print(data)
        async with session.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",json={"chat_id":chat_id,"text":text}) as resp:
            answer = await resp.json()

async def init_db(path):
    async with aiosqlite.connect(path) as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS edits_db (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            wiki TEXT,
            user TEXT,
            timestamp INTEGER,
            comment TEXT,
            delta INTEGER,
            revision_old INTEGER,
            revision_new INTEGER,
            url TEXT,
            diff TEXT,
            before_version TEXT,
            after_version TEXT
        )
    """)
        await conn.commit()

path_db = ''
env_token = ''
chat_id = ''
bot_token = ''
if args.database != 'None':
    path_db = args.database
    asyncio.run(init_db(path_db))

if args.env != 'None':
    env_token = dotenv.load_dotenv(args.env)
    bot_token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_BOT_CHATID")


async def fetch_revisions(old_id,new_id,lang):
    changes = []
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "revids": f"{old_id}|{new_id}",
        "rvslots": "*",
        "rvprop": "content"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://{lang}.wikipedia.org/w/api.php", params=params) as resp:
                               content = await resp.json()
                               changes = next(iter(content['query']['pages'].values()))["revisions"]
    edit = difflib.unified_diff(changes[0]["slots"]["main"]["*"].splitlines(),changes[1]["slots"]["main"]["*"].splitlines())
    edit_processed = "\n".join(edit)
    return edit_processed,changes


async def analyzestream(thread):
    while True:
        try:
            wikidata,lang = await queue.get()
            data = json.loads(wikidata[5:])
            delta = 0
            if 'length' in data:
                delta = data['length']['new']-data['length']['old']
            if data.get("namespace") == 0 and data.get("bot") != True and abs(delta) > 30:
                if watchlist_titles == [] or data['title'] in watchlist_titles:
                    edit_text = f"{data['title']} | Size: {delta} bytes | Comment {data['parsedcomment']}, by worker: {thread}"
                    log.info(edit_text)
                    fetchrev = await fetch_revisions(data['revision']['old'],data['revision']['new'],lang)
                    if path_db != '':
                        asyncio.create_task(write_to_sql(path_db,data,fetchrev[0],fetchrev[1]))
                    if bot_token != '':
                        asyncio.create_task(send_to_bot(bot_token,edit_text))
        except Exception as e:
            log.error(f"Exception {e} by worker{thread}")
            log.error(data)
        finally:
            queue.task_done()

async def fetchstream(wikicode):
    lang = f'"wiki":"{wikicode}wiki"'.encode()
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get('https://stream.wikimedia.org/v2/stream/recentchange') as resp:
                    async for raw_line in resp.content:
                        if b'"type":"edit"'in raw_line and lang in raw_line:
                            line = raw_line.decode("utf-8").strip()
                            await queue.put((line,wikicode))
        except aiohttp.ClientPayloadError as e:
            log.warning(f"Stream connection lost {e}, reconnecting in 3s")
            await asyncio.sleep(3)

async def write_to_sql(path,data, changes,full_edit):
       async with aiosqlite.connect(path) as conn:
           await conn.execute("""
            INSERT INTO edits_db (title, wiki, user, timestamp, comment, delta, revision_old, revision_new, url, diff, before_version, after_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)
        """, (
            data.get("title"),
            data.get("wiki"),
            data.get("user"),
            data.get("timestamp"),
            data.get("comment"),
            data["length"]["new"] - data["length"]["old"],
            data["revision"]["old"],
            data["revision"]["new"],
            data.get("notify_url"),
            changes,
            full_edit[0]["slots"]["main"]["*"],
            full_edit[1]["slots"]["main"]["*"]
            ))
           await conn.commit()


queue = asyncio.Queue()
async def main(num_workers=8):
    workers = []
    streamers = []
    for i in range(num_workers):
        workers.append(asyncio.create_task(analyzestream(i)))
    for lang in watchlist_langs:
        streamers.append(asyncio.create_task(fetchstream(lang)))
    for i in streamers:
        await i
if __name__ == "__main__":
    asyncio.run(main())

#asyncio.run(setup_bot(bot_token))
