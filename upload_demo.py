import httpx
from pathlib import Path
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor,as_completed
url = 'http://127.0.0.1:12555'
class upload:
    def __init__(self,service:str) -> None:
        self.url = service
        self.upload_pool = ThreadPoolExecutor(max_workers=20)

    def _get_upload_id(self,file:Path=None):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        data = {
            'uniq_id': '1233',
        }
        return httpx.post(self.url + f'/create_uploadid', headers=headers, data=data,timeout=120).json()
        
    def _upload_slice(self,slice:bytes,upload_id:str,slice_index:int,md5:str):

        headers = {
            'accept': 'application/json',
            'Content-Type': f'multipart/form-data; boundary={121313}',
        }
        files = {
            'file': slice
        }
        return httpx.put(self.url + f'/file-slice:{upload_id}/{slice_index}', headers=headers, files=files,timeout=15,params={"md5":md5 if md5 else None})

    def _merge(self,upload_id:str,key:str,size:int=None,md5:str=None):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'size': size,
            'md5': md5,
            'key': key,
        }
        return httpx.post(self.url + f'/file-merge:{upload_id}', headers=headers, data=data,timeout=120).json()

    def upload(self,file:str,key:str):
        upload_id = self._get_upload_id(file).get('upload_id')
        md5sum = hashlib.md5()
        fus = []
        all_size = os.stat(file).st_size
        with open(file,'rb') as f:
            index = 0
            size = 0
            while True:
                data = f.read(64*1024*1024)
                if len(data) == 0:
                    break
                size+=len(data)
                item_md5 = hashlib.md5()
                item_md5.update(data)
                fus.append(self.upload_pool.submit(self._upload_slice,slice=data,upload_id=upload_id,slice_index=index,md5=item_md5.hexdigest()))
                index += 1
                md5sum.update(data)
            load_size = 0
            for i in as_completed(fus):
                load_size+=i.result().json().get('file_size')
                print(f'{load_size:11d}:{all_size:11d}\r',flush=False,end='')
            self._merge(upload_id=upload_id,key=key,size=size,md5=md5sum.digest().hex())
        
if __name__ == '__main__':
    uploader = upload(url)
    uploader.upload('test','122')
