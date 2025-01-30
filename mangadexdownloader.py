import os
import requests
import img2pdf
import re
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', '_', name)

def get_manga_info(manga_id):
    url = f"https://api.mangadex.org/manga/{manga_id}"
    headers = {'User-Agent': 'MangaDexDownloader/1.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    title = data['data']['attributes']['title'].get('en') or list(data['data']['attributes']['title'].values())[0]
    return sanitize_filename(title)

def get_manga_id(url):
    path = urlparse(url).path
    parts = path.split('/')
    if len(parts) >= 3 and parts[1] == 'title':
        return parts[2]
    raise ValueError("Invalid MangaDex URL")

def fetch_all_chapters(manga_id):
    chapters = []
    offset = 0
    limit = 100
    headers = {'User-Agent': 'MangaDexDownloader/1.0'}
    
    while True:
        url = f"https://api.mangadex.org/manga/{manga_id}/feed"
        params = {
            "limit": limit,
            "offset": offset,
            "includes[]": ["scanlation_group"],
            "order[chapter]": "asc"
        }
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        chapters.extend(data['data'])
        
        if len(data['data']) < limit:
            break
        offset += limit
    return chapters

def select_language(chapters):
    languages = set()
    for chapter in chapters:
        lang = chapter['attributes']['translatedLanguage']
        languages.add(lang)
    print("\nAvailable languages:", ", ".join(sorted(languages)))
    selected = input("Enter language code to download (e.g. 'en'): ").strip()
    if selected not in languages:
        raise ValueError("Selected language not available")
    return selected

def filter_chapters(chapters, language):
    return [chap for chap in chapters 
            if chap['attributes']['translatedLanguage'] == language]

def group_chapters_by_number(chapters):
    groups = {}
    for chap in chapters:
        num = chap['attributes'].get('chapter')
        if num:
            # Convert to float for proper numeric comparison
            groups.setdefault(str(float(num)), []).append(chap)
    return groups

def get_scanlation_groups(chapter):
    groups = []
    for rel in chapter['relationships']:
        if rel['type'] == 'scanlation_group':
            name = rel['attributes'].get('name', rel['id'])
            groups.append(name)
    return groups or ['Unknown Group']

def analyze_groups(grouped_chapters, chapter_range):
    group_counts = defaultdict(int)
    group_entries = defaultdict(list)

    for chap_num in chapter_range:
        if chap_num in grouped_chapters:
            for entry in grouped_chapters[chap_num]:
                groups = get_scanlation_groups(entry)
                if groups:
                    main_group = groups[0]
                    group_counts[main_group] += 1
                    group_entries[chap_num].append({
                        'group': main_group,
                        'entry': entry,
                        'createdAt': entry['attributes']['createdAt']
                    })

    # Sort groups by count then by creation date
    sorted_groups = sorted(group_counts.items(), 
                         key=lambda x: (-x[1], x[0]))
    
    # Sort entries for each chapter by group preference
    for chap in group_entries.values():
        chap.sort(key=lambda x: (-group_counts[x['group']], x['createdAt']), 
                reverse=True)
    
    return group_entries, [g[0] for g in sorted_groups]

def get_image_urls(chapter_id, quality='data'):
    headers = {'User-Agent': 'MangaDexDownloader/1.0'}
    try:
        at_home_url = f"https://api.mangadex.org/at-home/server/{chapter_id}"
        response = requests.get(at_home_url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        base_url = data['baseUrl']
        chapter_data = data['chapter']
        
        if quality == 'data' and 'data' in chapter_data:
            filenames = chapter_data['data']
        elif quality == 'dataSaver' and 'dataSaver' in chapter_data:
            filenames = chapter_data['dataSaver']
        else:
            return None, None, None
        
        return base_url, filenames, chapter_data['hash']
    except:
        return None, None, None

def download_image(args):
    index, url, output_path = args
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        return False

def download_and_convert_chapter(chapter_entry, chapter_num, manga_folder, quality='data'):
    try:
        base_url, filenames, hash = get_image_urls(chapter_entry['entry']['id'], quality)
        if not filenames:
            return False

        temp_dir = os.path.join(manga_folder, f"temp_ch_{chapter_num}")
        os.makedirs(temp_dir, exist_ok=True)
        
        tasks = []
        for idx, filename in enumerate(filenames, 1):
            if quality == 'data':
                url = f"{base_url}/data/{hash}/{filename}"
            else:
                url = f"{base_url}/data-saver/{hash}/{filename}"
            
            ext = os.path.splitext(filename)[1]
            output_path = os.path.join(temp_dir, f"{idx:03d}{ext}")
            tasks.append((idx, url, output_path))
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(download_image, tasks))
        
        if all(results):
            images = sorted([os.path.join(temp_dir, f) for f in os.listdir(temp_dir) 
                           if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))])
            
            pdf_path = os.path.join(manga_folder, f"Chapter_{chapter_num}.pdf")
            with open(pdf_path, "wb") as f:
                f.write(img2pdf.convert(images))
            
            # Cleanup temporary files
            for img in images:
                os.remove(img)
            os.rmdir(temp_dir)
            return True
        return False
    except Exception as e:
        print(f"Error processing Chapter {chapter_num}: {str(e)}")
        return False

def main():
    try:
        url = input("Enter MangaDex URL: ").strip()
        manga_id = get_manga_id(url)
        manga_title = get_manga_info(manga_id)
        
        print(f"\nManga Title: {manga_title}")
        print("Fetching chapters...")
        chapters = fetch_all_chapters(manga_id)
        lang = select_language(chapters)
        
        filtered = filter_chapters(chapters, lang)
        grouped = group_chapters_by_number(filtered)
        
        # Get sorted chapters as floats but maintain string representation
        sorted_chapters = sorted(grouped.keys(), key=lambda x: float(x))
        print("\nAvailable chapters:", ", ".join(sorted_chapters))
        
        start = input("Enter start chapter number: ").strip()
        end = input("Enter end chapter number: ").strip()
        
        # Convert to float for numeric comparison
        chapter_range = [chap for chap in sorted_chapters 
                       if float(start) <= float(chap) <= float(end)]
        
        print("\nAnalyzing scanlation groups...")
        group_entries, preferred_groups = analyze_groups(grouped, chapter_range)
        
        print("\nDetected scanlation groups (in order of preference):")
        for i, group in enumerate(preferred_groups[:5], 1):
            print(f"{i}. {group}")
        
        quality = input("\nSelect quality (1. High 2. Data Saver): ").strip()
        quality = 'data' if quality == '1' else 'dataSaver'
        
        default_output = os.path.join("downloads", manga_title)
        output_dir = input(f"Enter output directory (default: {default_output}): ").strip() or default_output
        os.makedirs(output_dir, exist_ok=True)
        
        # Download chapters with fallback groups
        for chap_num in chapter_range:
            if chap_num in group_entries:
                entries = group_entries[chap_num]
                success = False
                
                for entry in entries:
                    print(f"\nAttempting Chapter {chap_num} ({entry['group']})...")
                    if download_and_convert_chapter(entry, chap_num, output_dir, quality):
                        print(f"Successfully downloaded Chapter {chap_num}")
                        success = True
                        break
                    else:
                        print(f"Failed with {entry['group']}, trying next group...")
                
                if not success:
                    print(f"All groups failed for Chapter {chap_num}")
            else:
                print(f"\nChapter {chap_num} not found in any group")
        
        print("\nDownload completed! PDFs saved in:", output_dir)
    
    except Exception as e:
        print(f"\nFatal Error: {str(e)}")

if __name__ == "__main__":
    main()