from datetime import date
import os
from urllib.parse import urlparse
from openai import OpenAI
import requests, time
from pathlib import Path
import simplejson as json
from bs4 import BeautifulSoup
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()

CURRENT_MONTH =os.getenv("CURRENT_MONTH")
CURRENT_MONTH_ID = os.getenv("CURRENT_MONTH_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
OPENAI_KEY = os.getenv("OPENAI_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

Path(f"data/{CURRENT_MONTH_ID}").mkdir(parents=True, exist_ok=True)
posts_tree = {}
if not Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").exists():
    Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").write_text('{}')
else:
    posts_tree = json.loads(Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").read_text())

existing_children = {}

if CURRENT_MONTH_ID in posts_tree:
    print(f"posts_tree already has {CURRENT_MONTH_ID} in it…")
    print(f"{CURRENT_MONTH_ID} already has {len(posts_tree[CURRENT_MONTH_ID]['children'])} children stored, backing up…")
    existing_children = posts_tree[CURRENT_MONTH_ID]['children']

def get_tree(item_id, tree, depth=0):
    if str(item_id) not in tree or depth == 0:
        print(f"{"  " * depth}Fetching {item_id}…")
        tree[item_id] = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", timeout=30
        ).json()
        time.sleep(0.25)
        if ('deleted' not in tree[item_id] or not tree[item_id]['deleted']) and 'kids' in tree[item_id]:
            print(f"{"  " * depth}{item_id} has {len(tree[item_id]['kids'])} kids…")
            if 'children' not in tree[item_id]:
                tree[item_id]['children'] = existing_children if depth == 0 else {}
            for kid in tree[item_id]['kids']:
                get_tree(kid, tree[item_id]['children'], depth=depth+1)
        else:
            print(f"{"  " * depth}No kids for {item_id} or it is a deleted item…")
    else:
        pass

get_tree(CURRENT_MONTH_ID, posts_tree)
json.dump(posts_tree, Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").open('w'), indent=2)

# Load the prompt
PROMPT_PATH = Path("prompts/evaluate_job.txt")
prompt_template = PROMPT_PATH.read_text()
SYSTEM_PROMPT_PATH = Path("prompts/evaluate_job_system.txt")
system_prompt = SYSTEM_PROMPT_PATH.read_text()

def get_comments_text_for_prompt(item, level=0):
    output = ""
    if 'children' in item:
        for comment_id in item['children']:
            comment = item['children'][comment_id]
            if 'deleted' not in comment or not comment['deleted']:
                output += f"\nComment ID {comment['id']} by {comment['by']} "
                output += f" on {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(comment['time']))}"
                if 'parent' in comment:
                    output += f" in reply to comment ID {comment['parent']}"
                output += ":\n"
                output += BeautifulSoup(comment['text'], features="html.parser").text
                output += "\n\n============\n" 
                if 'children' in comment:
                    output += get_comments_text_for_prompt(comment, level=level+1)
    return output

def get_tags_text_for_prompt():
    tags = set()
    for post_id in posts_tree[CURRENT_MONTH_ID]['children']:
        post = posts_tree[CURRENT_MONTH_ID]['children'][post_id]
        tags.update(post['tags'])
    return ", ".join(tags) if tags else "None"

for post_id in posts_tree[CURRENT_MONTH_ID]['children']:
    post = posts_tree[CURRENT_MONTH_ID]['children'][post_id]
    if ('deleted' not in post or not post['deleted']) and 'evaluation' not in post:
        comments_text = get_comments_text_for_prompt(post)
        prompt = prompt_template.format(
            month=CURRENT_MONTH,
            posting_id=post['id'],
            posting=BeautifulSoup(post['text'], features="html.parser").text,
            author=post['by'],
            posted_on=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(post['time'])),
            comments=comments_text,
            tags=", ".join([])
        )
        response = OpenAI(api_key=OPENAI_KEY).chat.completions.create(
            model="chatgpt-4o-latest",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            user=post_id
        )
        answer = response.choices[0].message.content
        answer = answer.replace("\n", "").replace("\t", "").replace("```json", "").replace("```", "")
        post['evaluation'] = json.loads(answer)
        json.dump(
            posts_tree,
            Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").open("w"),
            indent=2,
        )
        print(f"Evaluated {post_id}…")
        time.sleep(0.2)

client = Client(auth=NOTION_TOKEN)
db = client.databases.retrieve(NOTION_DATABASE_ID)

for post_id in posts_tree[CURRENT_MONTH_ID]['children']:
    post = posts_tree[CURRENT_MONTH_ID]['children'][post_id]
    if ('deleted' not in post or not post['deleted']) and 'evaluation' in post and ('notion_page_id' not in post or post['notion_page_id'] == None):
        print(f"Creating job prospect for {post_id}…")
        notion_o = {"parent": {"database_id": db['id']}}
        notion_o["properties"] = {}
        links_in_text = BeautifulSoup(post['text'], features="html.parser").find_all("a", href=True) 
        link_audit = set()
        relevant_links = []
        for link_index, link in enumerate(links_in_text):
            if link["href"] not in link_audit and link["href"].startswith("http"):
                link_href = link["href"]
                link_text = link.text
                if link_href == link_text:
                    print(f"Trying {link_href}…")
                    link_url = urlparse(link_href)
                    if link_url.hostname == "x.com" or link_url.hostname == "www.x.com":
                        link_text = "Twitter"
                    elif str(link_url.hostname).endswith("linkedin.com"):
                        link_text = "LinkedIn"
                    elif str(link_url.hostname).endswith("facebook.com"):
                        link_text = "Facebook"
                    elif str(link_url.hostname).endswith("wellfound.com"):
                        link_text = "Wellfound"
                    else:
                        try:
                            link_text = BeautifulSoup(requests.get(link_href, timeout=15).text, features="html.parser").find("title").text
                        except (
                            requests.exceptions.ConnectTimeout,
                            requests.exceptions.ConnectionError,
                            AttributeError,
                        ):
                            pass
                if len(relevant_links) != 0:
                    relevant_links.append({"text": {"content": "\n"}})
                relevant_links.append({
                    "text": {
                        "content": link_text,
                        "link": {
                            "url": link_href
                        }
                    }
                })
                link_audit.add(link_href)

        p = notion_o["properties"]
        p["Title"] = {"title": [{"text": {"content": post['evaluation']['company_name'] + " – " + post['evaluation']['position']}}]}
        p["Company Name"] = {"rich_text": [{"text": {"content": post['evaluation']['company_name']}}]}
        p["Source"] = {"url": "https://news.ycombinator.com/item?id=" + str(post['id'])}
        p["Impact Level"] = {"number": post['evaluation']['impact_level']}
        p["Impact Level Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['impact_level_reason']}}]}
        p["Location"] = {"rich_text": [{"text": {"content": post['evaluation']['location']}}]}
        p["Location Category"] = {"select": {"name": post['evaluation']['location_category']}}
        p["Location Fitment Score"] = {"number": post['evaluation']['location_fitment_score']}
        p["Location Fitment Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['location_fitment_score_reason']}}]}
        p["Overall Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['overall_reasoning']}}]}
        p["Position"] = {"rich_text": [{"text": {"content": post['evaluation']['position']}}]}
        p["Position Fitment Score"] = {"number": post['evaluation']['position_fitment_score']}
        p["Position Fitment Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['position_fitment_score_reason']}}]}
        post_content = BeautifulSoup(post['text'], features="html.parser").text
        p["Post Content"] = {
            "rich_text": [
                {
                    "text": {
                        "content": post_content[:1999]
                        + ("…" if len(post_content) > 1999 else "")
                    }
                }
            ]
        }
        p["Role Fitment Score"] = {"number": post['evaluation']['role_fitment_score']}
        p["Role Fitment Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['role_fitment_score_reason']}}]}
        p["Tags"] = {"multi_select": [{"name": tag} for tag in post['evaluation']['tags']]}
        p["Tech Fitment Score"] = {"number": post['evaluation']['tech_fitment_score']}
        p["Tech Fitment Reasoning"] = {"rich_text": [{"text": {"content": post['evaluation']['tech_fitment_score_reason']}}]}
        p["Total Rating"] = {"number": post['evaluation']['total_rating']}
        p["Well Funded"] = {"checkbox": bool(post['evaluation']['well_funded'])}
        p["Posted on"] = {"date": {"start": date.fromtimestamp(post['time']).strftime("%Y-%m-%d")}}
        p["HN Post Month"] = {"select": {"name": CURRENT_MONTH}}
        p["Relevant Links"] = {
            "rich_text": relevant_links
        }

        post['notion_page_id'] = client.pages.create(parent=notion_o["parent"], properties=notion_o["properties"])["id"]
        json.dump(
            posts_tree,
            Path(f"data/{CURRENT_MONTH_ID}/posts_tree.json").open("w"),
            indent=2,
        )
        print(f"Created job prospect for {post_id}…")
        time.sleep(0.2)
