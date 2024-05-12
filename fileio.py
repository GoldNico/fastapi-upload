import os
from pathlib import Path
import aiofiles
import hashlib
from fastapi import FastAPI,UploadFile,params,BackgroundTasks
from asyncio import get_event_loop
from time import sleep
app = FastAPI(docs_url="/docs")
base_dir = os.path.dirname(os.path.abspath(__file__))
upload_file_path = Path(base_dir, './uploads')
if upload_file_path.exists() is False:
    os.mkdir(upload_file_path)
tunk = 4*1024*1024 # 4M
def clear_upload_id(upload_id,timeout:int=1800):
    sleep(timeout)
    path = Path(upload_file_path,upload_id)
    if path.exists() is False:
        return
    path,_,files = next(os.walk(path))
    for slice_file in files:
        slice_file_path = Path(path,slice_file)
        os.remove(slice_file_path)
    os.rmdir(path)

@app.post('/create_uploadid')
async def create_uploadid(
    background_tasks: BackgroundTasks,
    uniq_id:str = params.Form(None,description="文件的 uniq_id ID")
):
    upload_id = uniq_id
    path = Path(upload_file_path,upload_id)
    if path.exists() is False:
        os.mkdir(path)
        background_tasks.add_task(clear_upload_id,upload_file_path)
    return {"upload_id":upload_id}


@app.put("/file-slice:{upload_id}/{index}",description="进行文件的分片上传接口")
async def upload_file_slice(
    upload_id: str = params.Path(...,description="文件id"),
    index: int = params.Path(..., description="文件分片序号"),
    file: UploadFile = params.File(..., description="具体文件"),
    md5:str = params.Param(...,description="验证对应的 md5值")
):
    """文件分片上传"""
    path = Path(upload_file_path, upload_id)
    if not os.path.exists(path):
        back = {
            'code': 404,
            'index':index,
            'upload_id': upload_id,
            'file_size':0,
            'msg':f'upload_id {upload_id} loss'
        }
    else:
        file_name = f'slice.{index}'
        file_path = Path(path,file_name)
        if md5 is None:
            serve_md5 = None
        else:
            serve_md5 = hashlib.md5()

        save_file = True
        if os.path.exists(file_path) and md5 is not None:
            async with aiofiles.open(file_path,'rb') as f:
                size = 0
                while True:
                    data =await f.read(tunk)
                    if len(data) == 0:
                        break
                    size += len(data)
                    serve_md5.update(data)
                if serve_md5.digest() == bytes.fromhex(md5):
                    save_file = False
                    back = {
                        'code': 200,
                        'index':index,
                        'upload_id': upload_id,
                        'file_size':size,
                        'msg':'pass'
                    }
        if save_file:
            async with aiofiles.open(file_path,'wb') as f:
                size = 0
                while True:
                    data = await file.read(tunk)
                    if len(data) == 0:
                        break
                    size += len(data)
                    if serve_md5 is not None:
                        serve_md5.update(data)
                    await f.write(data)
            
            back = {
                'code': 200,
                'index': index,
                'upload_id': upload_id,
                'file_size': size,
                'msg':'ok'
            }
    return back

@app.post("/file-merge:{upload_id}",description="将上传的文件片进行合并的接口")
async def merge_file(
    upload_id: str = params.Path(...,description="文件id"),
    size:int =  params.Form(None,description="文件的大小"),
    md5:str = params.Form(None,description="如果有则验证对应的 md5值"),
    key:str = params.Form(...,description="保存的路径值")
):
    """文件分片上传"""
    path = Path(upload_file_path,upload_id)
    file_path = Path(upload_file_path,key)
    if not os.path.exists(path):
        back = {
            'code': 404,
            'key':key,
            'upload_id': upload_id,
            'file_size':0,
            'msg':f'upload_id {upload_id} loss',
            'md5':None
        }
    else:
        serve_md5 = hashlib.md5()
        if os.path.isdir(path):
            path,_,files = next(os.walk(path))
            files.sort()
            async with aiofiles.open(file_path,'wb') as main_file:
                servr_size = 0
                for slice_file in files:
                    slice_file_path = Path(path,slice_file)
                    async with aiofiles.open(slice_file_path,'rb') as load_file:
                        while True:
                            data = await load_file.read(tunk)
                            if len(data) == 0:
                                break
                            else:
                                servr_size += len(data)
                                serve_md5.update(data)
                                await main_file.write(data)
                    os.remove(slice_file_path)
                print(path)
                os.rmdir(path)
            back = {
                'code': 200,
                'key':key,
                'upload_id': upload_id,
                'file_size':servr_size,
                'msg':f'ok',
                'md5':serve_md5.digest().hex()
            }
            if size is not None:
                if servr_size != size:
                   back = {
                        'code': 500,
                        'key':key,
                        'upload_id': upload_id,
                        'file_size':servr_size,
                        'msg':f'size err',
                        'md5':serve_md5.digest().hex()
                    } 
            if md5 is not None:
                if serve_md5.digest() != bytes.fromhex(md5):
                    back = {
                        'code': 500,
                        'key':key,
                        'upload_id': upload_id,
                        'file_size':servr_size,
                        'msg':f'md5 err',
                        'md5':serve_md5.digest().hex()
                    }
        else:
            os.remove(path)
            back = {
                'code': 500,
                'key':key,
                'upload_id': upload_id,
                'file_size':0,
                'msg':f'upload_id {upload_id} error',
                'md5':None
            }
    return back


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app, host="0.0.0.0", port=12555)