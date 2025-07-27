# WikiStalker

**Wikistalker** is a real-time Wikipedia edit monitoring tool based on Wikimedia EventStreamAPI.  
Track every change on articles connected to important people, organizations, or any Wikidata entity—across any language edition of Wikipedia. Receive Telegram alerts, store detailed edit logs in sqlite3 DB.

---
## Features

- Real-time Wikipedia edit monitoring via EventStream API
- Track by article titles, or automatically collect all articles linked to a Wikidata QID
- Optional Telegram alerts for new edits
- Optional SQLite database logging (with full diffs and metadata)
- Interactive menu for creating and managing listeners
- Supports multiple simultaneous listeners

---

## Installation

1. **Clone the repo:**
   ```bash
   git clone https://github.com/lugalanne/wikistalker.git
   cd wikistalker
````

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```
t
   Requires **Python 3.8+** and `pip`.

3. **(Optional) Set up a [Telegram bot](https://core.telegram.org/bots#creating-a-new-bot):**

   * Talk to [@BotFather](https://t.me/botfather), create a bot, and save your token.
   * Find your chat ID (see [here](https://stackoverflow.com/a/32423837) for help).
---

## Usage

### 1. **Start the interactive CLI menu:**

```bash
python ws_menu.py
```

Follow the menu prompts to:

* Create new monitoring listeners
* Enable Telegram alerts (optional)
* Store results in a database (optional)
* Start or stop listeners at any time

### 2. **Direct command-line usage:**

```bash
python wikistalker.py --wikis en,de,ru --titles titles.txt --database edits.db --env mybot.env
```

**Arguments:**

* `--wikis`: Comma-separated list of language codes (e.g., `en,ru,uk`).
* `--titles`: Path to a file listing article titles to monitor (one per line).
  Leave this blank or set `None` to monitor **all** articles.
* `--database`: Path to SQLite DB for storing edits.
* `--env`: Path to a `.env` file with Telegram credentials.

### 3. **Configuring Telegram Alerts**

Create a `.env` file like this (or it can be created from ws_menu.py):

```
TG_TOKEN=123456789:ABCDEF...
TG_CHAT_ID=123456789
```

---

## Example: Monitoring Edits for a Political Figure

Suppose you want to track all Wikipedia edits related to **Vladimir Putin** (Q7747) in English and Russian:

1. Use the menu’s "Collect article titles" for QID `Q7747` to get all connected article titles.
2. Save the titles list.
3. Create a new listener:
   * Languages: `en,ru`
   * Titles path: your saved titles file
   * Enable Telegram: Yes (enter bot token & chat_id)
   * Enable DB: Yes (choose DB path)
4. Start the listener.

---

## Database

* Edits are logged to a SQLite database with:

  * QID/title
  * Language
  * Edit timestamp and user
  * Size delta and comments
  * Before/after wikitext
  * Full revision diff
---

## Requirements

* Python 3.8+
* GNU/Linux (tested)
* [See `requirements.txt`](requirements.txt)

---

## License

[MIT License](LICENSE) This project is licensed under the MIT License.

---

## Roadmap & Known Issues

* [ ] Making connected entities for list of QIDs
* [ ] WikiData properties optimization
* [ ] Refactoring spaghetti code
* [ ] Config file editor in menu
* [ ] Smarter diff/alert rules (future)
* [ ] Analytical mode with NLP (with SpaCy maybe)

---

## Contributing

Pull requests are welcome! Please open an issue first if you’d like to discuss major changes.

---

## Credits

* Wikipedia & Wikidata EventStreams API
* [aiohttp](https://docs.aiohttp.org/)
* [Rich](https://github.com/Textualize/rich)
* [python-dotenv](https://github.com/theskumar/python-dotenv)
* All open-source contributors

---

*WikiStalker is not affiliated with Wikimedia Foundation.*
