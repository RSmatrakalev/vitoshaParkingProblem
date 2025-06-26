import os
import requests
import zipfile
import shutil
from pathlib import Path

class CadastralDownloader:
    DISTRICT_BG = {
        'lozenets': 'Лозенец',
        'studentski': 'Студентски',
        'Lozenets': 'Лозенец',
        'Studentski': 'Студентски',
    }

    def __init__(self, district_slug: str):
        self.district_slug = district_slug.lower()
        if self.district_slug not in self.DISTRICT_BG:
            raise ValueError(
                f'Непознат район "{district_slug}". '
                'Добави го в DISTRICT_BG.'
            )
        self.district_bg = self.DISTRICT_BG[self.district_slug]

        self.base_dir = Path(__file__).resolve().parent
        self.downloads_base = self.base_dir / 'downloadsRowData'

    def download_data(self, url: str, filename: str):
        if not filename.endswith('.zip'):
            filename += '.zip'

        subfolder_path = self.downloads_base / filename.split('_')[0]
        subfolder_path.mkdir(parents=True, exist_ok=True)

        zip_path = subfolder_path / filename
        r = requests.get(url)
        if r.status_code != 200:
            print(f'[✘] Fail to download ({r.status_code})')
            return
        zip_path.write_bytes(r.content)
        print(f'[✔] Downloaded: {zip_path}')

        extract_path = subfolder_path / filename.replace('.zip', '')
        if extract_path.exists():
            shutil.rmtree(extract_path)
        extract_path.mkdir(parents=True)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            members = zf.infolist()
            top_level = os.path.commonprefix(
                [m.filename for m in members]
            ).split('/')[0]

            for member in members:
                if member.is_dir():
                    continue
                member_path = Path(member.filename)
                if member_path.parts[0] == top_level:
                    relative = Path(*member_path.parts[1:])
                else:
                    relative = member_path
                dest = extract_path / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, open(dest, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
        print(f'[✔] Unzipped (flattened) in: {extract_path}')

        zip_path.unlink()
        print(f'[🗑️ ] Deleted .zip: {zip_path}')

    def collect_all(self):
        base_url = "https://kais.cadastre.bg/bg/OpenData/Download?path="
        prefix = (
            "област София (столица)/община Столична/"
            f"гр. София (68134) - район {self.district_bg}/"
        )
        data_types = {
            "pozemleni_imoti": "поземлени имоти.zip",
            "sgradi": "сгради.zip",
            "samostoyatelni_obekti": "самостоятелни обекти.zip",
        }
        for key, zipped_file in data_types.items():
            url = base_url + prefix + zipped_file
            filename = f"{self.district_slug}_{key}"
            self.download_data(url, filename)

if __name__ == "__main__":
    CadastralDownloader("lozenets").collect_all()
    CadastralDownloader("studentski").collect_all()
