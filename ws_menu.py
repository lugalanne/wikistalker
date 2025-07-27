from rich.console import Console
from rich.table import Table
from rich.progress import Progress
import os,sys,subprocess,psutil
import requests,json

console = Console()

if_exit = False
wikiproperties = ["P53","P54","P102","P108","P463","P39","P69","P1416","P488","P749","P112","P169","P127","P22","P25","P26","P40","P451","P3373"]
def start():
    console.clear()
    console.print("WikiStalker v0.1a Monitor Wikipedia Edits in Real Time",style="bold red on white",justify='center')
    console.print("\n\n")
    console.print('1. Create new listener')
    console.print('2. Start listener')
    console.print('3. Show running listeners')   
    console.print("4. Collect list of article titles")
    console.print('5. Exit (without closing active listeners)')
    console.print('\n')
start()

def GetTitlesfromQID(qid,langset=('en','de','fr','es','it','ru','uk')):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    resp = requests.get(url)
    data = resp.json()
    entity = data['entities'][qid]
    sitelinks = entity['sitelinks']
    titles = [j['title'] for i,j in sitelinks.items() if i.endswith('wiki') and i[0:2] in langset]
    return titles

def GetQIDConnections(qid):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    resp = requests.get(url)
    data = resp.json()
    entity = data['entities'][qid]
    claims = entity['claims']
    connected_qids = set()
    for prop, claimlist in claims.items():
        for claim in claimlist:
            mainsnak = claim.get('mainsnak', {})
            datavalue = mainsnak.get('datavalue', {})
            if datavalue.get('type') == 'wikibase-entityid' and mainsnak['property'] in wikiproperties:
                value = datavalue['value']
                connected_qid = value.get('id')
                if connected_qid:
                    connected_qids.add(connected_qid)
    return connected_qids
def WriteTitlesToFile(titles, filename):
    seen = set()
    unique_titles = []
    for title in titles:
        if title not in seen:
            unique_titles.append(title)
            seen.add(title)
    with open(filename, "w", encoding="utf-8") as f:
        for title in unique_titles:
            f.write(f"{title}\n")
    print(f"Saved {len(unique_titles)} unique titles to {filename}")

def CheckConfig(filename='listeners.json'):
    listeners = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            listeners = json.load(f)
    except FileNotFoundError:
        listeners = []
    except ValueError:
        listeners = []
    finally:
        return listeners
def MakeJSON(name,langs,path_to_titles,db,env,background=True):
    new_listener = [{
        'in_background':background,
        "name": name,
        "wikies":langs,
        "path_titles":path_to_titles,
        "path_to_db":db,
        "path_to_env":env
    }]
    return new_listener


while if_exit != True:
    opt = console.input("Select an option: ")
    if opt == '4':
        i = console.input('Enter Wikidata QID (e.g. Q42) ')
        them_all = GetTitlesfromQID(i)
        k = GetQIDConnections(i)
        with Progress() as progress_bar:
            task = progress_bar.add_task("[cyan]Processing QIDs...",total=len(k))
            for t in k:
                titles = GetTitlesfromQID(t)
                them_all.extend(titles)
                progress_bar.update(task,advance=1)
        console.print(f'List of {len(them_all)} related entities is ready',style="cyan red")  
        name = console.input('Enter filename to save the list: ')
        WriteTitlesToFile(them_all, name)
        start()
    elif opt == '5':
        if_exit = True
    elif opt == '1':
        start()
        listener_name = console.input('Enter a name for the listener: ')
        os.mkdir(listener_name)
        langs = console.input('Enter target languages, separated by commas (e.g., en,ru,uk): ')
        console.print(langs.split(','))
        path_titles = console.input('Enter path to article titles watchlist (leave blank to monitor all edits): ')
        if path_titles == '':
            path_titles = 'None'
        bot_chatid,bot_token,env_path,db_path = '','','None','None'
        if console.input("Send alerts to Telegram? (y/n) ").lower() != 'n':
            bot_token = console.input('Enter your Telegram bot token: ')
            bot_chatid = console.input('Enter the Telegram chat_id or channel for alerts: ')
            env_path = f'{listener_name}/{listener_name}.env'
            with open(env_path,'w',encoding='utf-8') as f:
                f.write(f"TG_BOT_TOKEN={bot_token}\n")
                f.write(f"TG_BOT_CHATID={bot_chatid}\n")
        if console.input('Save edits to database? (y/n) ').lower() != 'n':
            db_path = listener_name+'/'+console.input("Input path to .db: ")
        console.print(f"Languages: {langs}\nTitles path: {path_titles}\n")
        if console.input('Confirm settings? (y/n):').lower() == 'y':
            existed_listeners = CheckConfig()
            new_listener = MakeJSON(listener_name,langs,path_titles,db_path,env_path)
            existed_listeners.append(new_listener)
            with open('listeners.json','w',encoding='utf-8')as f:
                json.dump(existed_listeners,f,ensure_ascii=False, indent=2)
            start()
            console.print('Listener created and ready to start')
            opt = '2'
        else:
            start()
    elif opt == '2':
        all_listeners = CheckConfig()
        j=0
        for i in all_listeners:
            console.print(f"{j}. {i[0]['name']} {i[0].get('wikies')} with nohup: {i[0].get('in_background')}")
            j+=1
        console.print(f"{len(all_listeners)}: Exit")
        run = console.input('Enter the number of the listener to start: ')
        if int(run) == len(all_listeners):
            start()
        else:
            listener_start = all_listeners[int(run)][0]
            back = ''
            if listener_start.get('in_background') == True:
                back = 'nohup'
            name = listener_start.get('name')
            wikis = listener_start.get('wikies')
            path_titles = listener_start.get('path_titles')
            pathdb = listener_start.get('path_to_db')
            pathenv = listener_start.get('path_to_env')
            logfile = open(f'listener_{name}.log','w')
            proc = subprocess.Popen([back,sys.executable,'wikistalker.py','--name',name,'--wikies',wikis,'--titles', path_titles,'--database', pathdb,'--env', pathenv], stdout=logfile,stderr=subprocess.STDOUT,close_fds=True)
            console.print(f'Listener {name} sucessfully started')
            opt = '3'
    elif opt == '3':
        active_listeners = []
        for proc in psutil.process_iter(['pid', 'cmdline']):
            if 'wikistalker.py' in str(proc.info['cmdline']):
                active_listeners.append(psutil.Process(proc.info['pid']))
        j = 1
        console.print('0. Go back')
        tab = Table(title="Monitor")
        tab.add_column("NUM")
        tab.add_column('PID')
        tab.add_column('Listener')
        tab.add_column('Started at')
        tab.add_column('Lasts for')
        for i in active_listeners: 
            tab.add_row(f"{j}",f"{i.pid}",f"{i.cmdline()}",f"{i.create_time()}",f"{i.cpu_times()}")
            j+=1
        console.print(tab)
        num =  console.input('Enter the number of a running listener to terminate, or 0 to go back: ') 
        if num != '0':
            active_listeners[int(num)-1].terminate()
            start()
        else:
            console.clear()
            start()
    else:
        console.print("Incorrect option")
