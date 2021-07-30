import fitz
import os
import json
import re
import numpy as np
from PIL import Image
import requests
import cv2
import pytesseract
import imutils
from statistics import stdev
from elasticsearch import Elasticsearch, helpers
from yolo_detection import detect_batch_frame
from txtai.pipeline import Similarity
import onnxruntime

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(hosts=["http://localhost:9200"], timeout=60, retry_on_timeout=True)


similarity = Similarity("valhalla/distilbart-mnli-12-3")

provider = os.getenv('PROVIDER', 'CUDAExecutionProvider')
# provider = os.getenv('PROVIDER', 'CPUExecutionProvider')
model = onnxruntime.InferenceSession("models/yolov5/lsq.onnx", providers=[provider])

def convert_img(page, zoom = 2.2):
    mat = fitz.Matrix(zoom, zoom)
    annot = page.annots()
    for a in annot:
      page.deleteAnnot(a)
    pix = page.getPixmap(matrix = mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img
    

def sort_block(blocks):
    result = []
    for block in blocks:
        if block['type'] == 0:
            x0 = str(int(block['bbox'][0] + 0.99999)).rjust(4, "0")
            y0 = str(int(block['bbox'][1] + 0.99999)).rjust(4, "0")

            sortkey = y0 + x0
            result.append([block, sortkey])
    
    result.sort(key = lambda x: x[1], reverse=False)
    return [i[0] for i in result]


def get_text(block):
    text = ''
    for lines in block['lines']:
        for span in lines['spans']:
            text += span['text']
    return text


def save_pdf(link, save_path = 'cfs_temp.pdf'):
    if "https://dmec.moh.gov.vn" in link:
        reponse = requests.get(link, verify=False)
        with open(save_path, 'wb') as fd:
            fd.write(reponse.content)
        return save_path
    return link

def rotation_check(img):
 
    # image = cv2.imread("TuTable/000197_Appendix_001.jpg")
    # gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # gray = cv2.bitwise_not(gray)
    rot_data = pytesseract.image_to_osd(img);
    # print("[OSD] "+rot_data)
    rot = re.search('(?<=Rotate: )\d+', rot_data).group(0)
    
    angle = float(rot)
    
    # rotate the image to deskew it
    rotated = imutils.rotate_bound(img, angle) #added
    
    return rotated, angle

def rotate_box(point, angle=0):

    return point

def annot_box(page,zoom):
    drawed = page.get_drawings()
    annot = page.annots()
    
    p = []

    for a in annot:
        point = a.rect
        point = point*zoom
        point = [point[0]-7, point[1]-3, point[2]+7, point[3]+3]
        p.append(point)

    for d in drawed:
        point = d['rect']
        point = point*zoom
        point = [point[0]-7, point[1]-3, point[2]+7, point[3]+3]
        p.append(point)
    
    return(p)

def pdfimage_process(pdf_path, check_annot=False):
    doc = fitz.open(pdf_path)
    images = []
    # points = []
    annots = []
    lsq_detect = False
    page_detect = None
    for i, page in enumerate(doc):
        print(i)
        zoom = 2.2
        list_point=[]
        if check_annot:
            p = annot_box(page, zoom)
            list_point = rotate_box(p, 0.0)
            # for k, pnt in enumerate(list_point):
            #     p_img = img1.crop(pnt)
            #     p_img.save(f'/content/crop_{i}_{k}.jpg')
        img = convert_img(page,zoom)
        label = detect_batch_frame(model, [cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)], image_size=(640,640))
        if len(label) > 0:
            lsq_detect = True
            page_detect = i+1
        # mat = fitz.Matrix(zoom, zoom)
        # pix = page.getPixmap(matrix = mat)
        # img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        # img = np.array(img)
        try:
            img, angle = rotation_check(img)
        except:
            img = img
            angle = 0.0
        # img1 = img
        # img = np.array(img)
        # cv2.imwrite(f'/content/original_{i}.jpg', img)
        images.append(cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR))
        
        # if check_annot:
        #     p = annot_box(page, zoom)
        #     list_point = rotate_box(p, angle)
        #     for k, pnt in enumerate(list_point):
        #         p_img = img1.crop(pnt)
        #         p_img.save(f'/content/crop_{i}_{k}.jpg')
        an_img = Image.new('RGB',(img.size[0],img.size[1]),(225,225,225))
        if len(list_point)>0:
        # for p in list_point:
            for k, p in enumerate(list_point):
                # p_img = img.crop(pnt)
                p_img = img.crop(p)

                # p_img = np.array(p_img)
                an_img.paste(p_img,(int(p[0]),int(p[1])))
                # p_img.save(f'/content/crop_{i}_{k}.jpg')
            # print(img.shape)
            # points = [np.array([[p[0],p[1]], [p[2],p[1]], [p[2],p[3]], [p[0],p[3]]],dtype=int) for p in list_point]
            # print(points)
            # _mask = np.zeros((img.shape[0],img.shape[1]), dtype=np.uint8)

            # mask = cv2.fillPoly(_mask, points, (255), 127, 0)
            # print(mask.shape)
            # result = cv2.bitwise_and(img, img, mask=mask)
            
            # cv2.imwrite(f'/content/img_{i}.jpg', result)
            # print(i)
            # p_img = img.crop(p)
            # p_img = np.array(p_img)
            annots.append(cv2.cvtColor(np.array(an_img), cv2.COLOR_RGB2BGR))
        else:
            annots.append(None)   

    return images, annots, (lsq_detect,page_detect)

def push_result(result):
    buffer = []
    x=0
    for res in result:
        for text in res: 
            # Article record
            article = {"_id": x, "_index": "articles", "title": text}
            x+=1
            # Buffer article
            buffer.append(article)
    print(buffer)
    if buffer:
        helpers.bulk(es, buffer)


def search(query, limit=1):
    query = {
        "size": limit,
        "query": {
            "query_string": {"query": query}
        }
    }

    results = []
    for result in es.search(index="articles", body=query)["hits"]["hits"]:
        # print(result)
        source = result["_source"]
        results.append(source["title"])

    return results
def del_data(idx='articles'):
    es.indices.refresh(index=idx)
    es.indices.delete(index=idx)
    

def ranksearch(query, limit):
  results = [text for _, text in search(query, limit * 10)]
  return [(score, results[x]) for x, score in similarity(query, results)][:limit]