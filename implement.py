import fitz
import json
import requests
from datetime import datetime
from fuzzywuzzy import fuzz
from dateutil import parser as pr
from utils import *
from CFS import *
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(hosts=["http://localhost:9200"], timeout=60, retry_on_timeout=True)


def get_info_cfs(json_path, file_path):
    json_data = json.load(open(json_path))
    doc_type = 0
    # print(json_data)
    for attachment in json_data['attachmentList']:
        if attachment['code'] == 'CNLH':
            pdf_path = attachment['fileList'][0]['url']
            print(pdf_path)
            break
    
    resp = requests.get(pdf_path, verify=False)
    pdf_path = f'tmp/{file_path.split(".")[0]}.pdf'
    pdf_file = open(pdf_path, 'wb')
    pdf_file.write(resp.content)
    pdf_file.close()
    
    cfs = CFS()

    text = cfs.tool_reader(pdf_path)
    if "PHIẾU TIẾP NHẬN" in text:
        output = cfs.get_ptn_info(text)
        final_result, doc_type = cfs.final_result(query=json_data,result=output,ptn=True)
        
    else:
        vertical_results, horizontal_results, lsq_detect = cfs.deeplearning_reader(pdf_path)
        push_result(vertical_results)
        final_result, doc_type, err = cfs.final_result(query=json_data)
        print('lsq_detect: ', lsq_detect)
        if lsq_detect[0]:
            final_result.append({
                "commentContent" : f"Có dấu lãnh sứ quán (trang {lsq_detect[1]})",
                "commentStatus" : "OK"
            })
        else:
            final_result.append({
                "commentContent" : f"Không có dấu lãnh sứ quán)",
                "commentStatus" : "NOK"
            })
            err.append(7)
    
    return final_result, err
        


    # doc = fitz.open(pdf_path)
    # text = doc[0].getText("text")
    # # print(text)
    # comments = []
    # if "PHIẾU TIẾP NHẬN" in text:
    #     output = main(pdf_path)
    #     if fuzz.token_set_ratio(output["Production_company"], json_data["equipment"]['equipmentList'][0]["factoryList"][0]["name"]) > score:
    #         comments.append({
    #             "commentContent" : "Tên, địa chỉ cơ sở sản xuât hợp lệ (trang 1)",
    #             "commentStatus" : "OK"})
    #     else:
    #         comments.append({
    #             "commentContent" : "Tên, địa chỉ cơ sở sản xuât không hợp lệ (trang 1)",
    #             "commentStatus" : "NOK"})
    #     doc_type = 1
    # else:
    #     p = 0
    #     lsq_detect = False
    #     page_detect = 0

    #     #TODO review code
    #     list_check = [0 for i in range(len(json_data['equipment']))]
    #     list_flag = [0, 0]
    #     for page in doc:
    #         zoom = 2
    #         mat = fitz.Matrix(zoom, zoom)
    #         pix = page.getPixmap(matrix=mat)
    #         pix.writePNG(".tmp/test_ocr.png")
    #         pred = inference(model, ".tmp/test_ocr.png")
    #         if len(pred) > 0:
    #             lsq_detect = True
    #             page_detect = p
    #         result = reader.readtext(".tmp/test_ocr.png", detail=0)
            
    #         for text in result:

    #             fuzzy_score = fuzz.token_set_ratio(text, json_data["equipment"]['equipmentList'][0]["factoryList"][0]["name"])
    #             if fuzzy_score > score:
    #                 if list_flag[0] == 0:
    #                     comments.append({
    #                         "commentContent" : f"Công ty sản xuất hợp lệ (trang {p + 1})",
    #                         "commentStatus" : "OK"
    #                     })
    #                     list_flag[0] = 1
    #             fuzzy_score = fuzz.token_set_ratio(text, json_data["equipmentOwner"]['name'])
    #             if fuzzy_score > score:
    #                 if list_flag[1] == 0:
    #                     comments.append({ 
    #                         "commentContent" : f"Công ty sở hữu hợp lệ (trang {p + 1})",
    #                         "commentStatus" : "OK"
    #                     })
    #                     list_flag[1] = 1
    #             date_end = ''
                
    #             if "Expiry Date" in text:
    #                 date_end = text.replace("Expiry Date", "")

    #             if "valid from" in text and "to" in text:
    #                 date_end = text.split("to")[-1]
                
    #             if len(date_end) > 0:
    #                 print("------------------------------------")
    #                 date_end = "".join(u for u in date_end if u not in ("?", ".", ";", ":", "!"))
    #                 time_end = pr.parse(date_end.strip())
                    
    #                 if time_end > time:
    #                     comments.append({ 
    #                         "commentContent": f"Thời gian còn hiệu lực (trang {p + 1})",
    #                         "commentStatus" : "OK"
    #                     })
    #                 else:
    #                     comments.append({ 
    #                         "commentContent": f"Thời gian hết hiệu lực (trang {p + 1})",
    #                         "commentStatus" : "NOK"
    #                     })
              
    #             #TODO review code
    #             # for idx, obj in enumerate(json_data['equipment']):
    #             # print(json_data)
    #             obj = json_data['equipment']
    #             idx = 0
    #             if fuzz.token_set_ratio(text, obj['name']) > score:
    #                 if list_check[idx] == 0:
    #                     list_check[idx] = 1
    #         p += 1
    #     if 0 in list_check:
    #         comments.append({
    #             "commentContent" : f"Danh sách TTBYT ko đúng",
    #             "commentStatus" : "NOK"
    #         })
    #     else:
    #         comments.append({
    #             "commentContent" : f"Danh sách TTBYT đầy đủ",
    #             "commentStatus" : "OK"
    #         })
        
    #     for idx, flag in enumerate(list_flag):
    #         if flag == 0:
    #             if idx == 0:
    #                 comments.append({ 
    #                     "commentContent" : "Công ty sản xuất không hợp lệ",
    #                     "commentStatus" : "NOK",
    #                 })
    #             else:
    #                 comments.append({
    #                     "commentContent" : "Công ty sở hữu không hợp lệ",
    #                     "commentStatus" : "NOK"
    #                 })
    #     if lsq_detect:
    #         comments.append({
    #             "commentContent" : f"Có dấu lãnh sứ quán (trang {page_detect + 1})",
    #             "commentStatus" : "OK"
    #         })
    #     else:
    #         comments.append({
    #             "commentContent" : f"Không có dấu lãnh sứ quán)",
    #             "commentStatus" : "NOK"
    #         })
    # return comments, doc_type

if __name__ == '__main__':
    
    json_folder = 'input_dmec'
    checkpoint = []
    for file_path in os.listdir(json_folder):
        if 'p_' in file_path:
            type_file = 1
        else:
            type_file = 0
        json_file = os.path.join(json_folder,file_path)
        result, err = get_info_cfs(json_file,file_path)
        with open(os.path.join('output',file_path),'w') as output:
            json.dump(result,output)
        dic = {
          'file_name': file_path,
          'type': type_file,
          'err': err
        }
        checkpoint.append(dic)
    
    with open('checkpoint.json', 'w') as jsonfile:
        json.dump(checkpoint,jsonfile)
