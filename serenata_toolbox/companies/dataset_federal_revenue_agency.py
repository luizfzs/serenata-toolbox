import os
import re
import asyncio

import requests
import httpx
from tqdm import tqdm
from bs4 import BeautifulSoup


OPEN_DATA_CNPJ_URL = "http://receita.economia.gov.br/orientacao/tributaria/cadastros/" \
                     "cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj"


class FraDataset:
    def __init__(self,
                 url=OPEN_DATA_CNPJ_URL,
                 output_dir='/tmp/serenata-data/companies/',
                 skip_if_file_exists=False):
        self.url = url
        self.output_dir = output_dir
        self.skip_if_file_exists = skip_if_file_exists

    def get_link_list(self):
        response = requests.get(self.url)

        soup = BeautifulSoup(response.text, 'lxml')
        elements = soup.find('div', {'id': 'content-core'}).find_all('a', {
            'href': re.compile(r'DADOS_ABERTOS_CNPJ_(\d)+.zip')})

        return [element['href'] for element in elements]

    async def fetch(self, uri):
        path = f"{self.output_dir}/{uri.split('/')[-1]}"
        print(f"{path}")

        if self.skip_if_file_exists and os.path.exists(path):
            return

        async with httpx.AsyncClient().stream("GET", uri) as r:
            with open(path, 'wb') as f:
                t = tqdm(total=int(r.headers['Content-Length']) / 8, desc=uri,
                         position=0, unit='KiB', leave=True)
                async for data in r.aiter_bytes():
                    f.write(data)
                    t.update(len(data) / 8)

                t.close()

    async def download_dataset(self):
        self.setup_output_dir()
        links = self.get_link_list()
        tasks = [self.fetch(link) for link in links]
        await asyncio.gather(
            *tasks
        )

    def setup_output_dir(self):
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        if not os.path.isdir(self.output_dir):
            print(f"{self.output_dir} is a file. Cannot proceed.")

            exit(-1)

    def generate(self):
        print("Starting to fetch files to build the dataset")
        asyncio.run(self.download_dataset())
        print("Files retrieved")
        print("Starting to parse the raw files")
        # parse the files
        print("Finished parsing the raw files")


FraDataset().generate()
