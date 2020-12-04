import boto3
import io
from io import BytesIO
import sys
import math
import time
import json
import glob
from pdf2image import convert_from_path 
import os
from pathlib import Path
import re




# Displays information about a block returned by text detection and text analysis
def DisplayBlockInformation(block, counter):
    blockinfo =""
    if 'Text' in block:
        confidence = "N/A"
        if 'Confidence' in block:
            confidence = "{:.2f}".format(block['Confidence']) + "%"
        blockinfo = 'Line ' + str(counter) + ': "' + block['Text'] + '" with ' + confidence +' confidence\n'			
        #print('Line ' + str(counter) + ': "' + block['Text'] + '" with ' + confidence +' confidence')
    return blockinfo
    """
    print('Id: {}'.format(block['Id']))
    if 'Text' in block:
        print('    Detected: ' + block['Text'])
    print('    Type: ' + block['BlockType'])
   
    if 'Confidence' in block:
        print('    ****Confidence: ' + "{:.2f}".format(block['Confidence']) + "%")

    if block['BlockType'] == 'CELL':
        print("    Cell information")
        print("        Column:" + str(block['ColumnIndex']))
        print("        Row:" + str(block['RowIndex']))
        print("        Column Span:" + str(block['ColumnSpan']))
        print("        RowSpan:" + str(block['ColumnSpan']))    
    
    if 'Relationships' in block:
        print('    Relationships: {}'.format(block['Relationships']))
    print('    Geometry: ')
    print('        Bounding Box: {}'.format(block['Geometry']['BoundingBox']))
    print('        Polygon: {}'.format(block['Geometry']['Polygon']))
    
    if block['BlockType'] == "KEY_VALUE_SET":
        print ('    Entity Type: ' + block['EntityTypes'][0])
    
    if block['BlockType'] == 'SELECTION_ELEMENT':
        print('    Selection element detected: ', end='')

        if block['SelectionStatus'] =='SELECTED':
            print('Selected')
        else:
            print('Not selected')    
    
    if 'Page' in block:
        print('Page: ' + block['Page'])
    print()
    """

def analyze_document(document):

    #Get the document from S3
    #s3_connection = boto3.resource('s3',aws_access_key_id='XX',aws_secret_access_key='XXX',region_name='us-east-2')
                          
    #s3_object = s3_connection.Object(bucket,document)
    #s3_response = s3_object.get()

    #stream = io.BytesIO(s3_response['Body'].read())
    #image=Image.open(stream).convert('RGB')
    
    #use this to read from local file system and send AWS raw bytes so it does not     store it on the file system
    srcfile = open(document,"rb")
    stream = io.BytesIO(srcfile.read())
    #image=Image.open(stream).convert('RGB')
	

    # Analyze the document
    client = boto3.client('textract',region_name='us-east-2',aws_access_key_id='XX',aws_secret_access_key='XXX')
    
    image_binary = stream.getvalue()
	
    #note: AnalyzeDocument is a synchronous operation. To analyze documents asynchronously, use StartDocumentAnalysis .
	#Also note: when calling the AnalyzeDocument operation: HumanLoop doesn’t support images supplied as raw bytes. Use an image that is stored in an S3 bucket
	#Also note: synchronous calls don't support pdf, only jpg and png
    
    """Analyze document vs detect document txt
	https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html
	https://docs.aws.amazon.com/textract/latest/dg/API_DetectDocumentText.html
    """
    response = client.analyze_document(Document={'Bytes':     image_binary},    #HumanLoopConfig={"FlowDefinitionArn":"arn:aws:sagemaker:us-east-2:572037570099:flow-definition/bs-workflow","HumanLoopName":"bs-workflow",
                          #"DataAttributes" : {ContentClassifiers=["FreeOfPersonallyIdentifiableInformation"|"FreeOfAdultContent",]} },
        FeatureTypes=["TABLES", "FORMS"])
		
    with open('C:/Users/zabarano/Desktop/netandroid/analyze_document.json', 'w') as outfile:
        json.dump(response, outfile)
	
	
    """
    response = client.detect_document_text(Document={'Bytes':     image_binary}   )
    with open('C:/Users/zabarano/Desktop/netandroid/payload.json', 'w') as outfile:
        json.dump(response, outfile)
    """
    
    """
    # Alternatively, process using S3 object
    response = client.analyze_document(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}},HumanLoopConfig={"FlowDefinitionArn":"arn:aws:sagemaker:us-east-2:572037570099:flow-definition/bs-workflow",
                          "HumanLoopName":"bs-workflow",
                          #"DataAttributes" : {ContentClassifiers=["FreeOfPersonallyIdentifiableInformation"|"FreeOfAdultContent",]}
                         },
        FeatureTypes=["TABLES", "FORMS"])
    """
    
    #Get the text blocks
    blocks=response['Blocks']
    #width, height =image.size  
    #draw = ImageDraw.Draw(image)  
    print ('*********Analyzed Document***********')
    
    """	
    for counter, block in blocks:
        if block['BlockType'] == 'LINE':
            DisplayBlockInformation(block, counter)       
    """    
    return len(blocks)

	
	
def detect_document(document):

    #Get the document from S3
    #s3_connection = boto3.resource('s3',aws_access_key_id='XX',aws_secret_access_key='XXX',region_name='us-east-2')
                          
    #s3_object = s3_connection.Object(bucket,document)
    #s3_response = s3_object.get()

    #stream = io.BytesIO(s3_response['Body'].read())
    #image=Image.open(stream).convert('RGB')
    
    #use this to read from local file system and send AWS raw bytes so it does not     store it on the file system
    srcfile = open(document,"rb")
    stream = io.BytesIO(srcfile.read())
    #image=Image.open(stream).convert('RGB')
	

    # Analyze the document
    client = boto3.client('textract',region_name='us-east-2',aws_access_key_id='XX',aws_secret_access_key='XXX')
    
    image_binary = stream.getvalue()
	
    #note: AnalyzeDocument is a synchronous operation. To analyze documents asynchronously, use StartDocumentAnalysis .
	#Also note: when calling the AnalyzeDocument operation: HumanLoop doesn’t support images supplied as raw bytes. Use an image that is stored in an S3 bucket
    #Also note: synchronous calls don't support pdf, only jpg and png
    
    """Analyze document vs detect document txt
	https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html
	https://docs.aws.amazon.com/textract/latest/dg/API_DetectDocumentText.html
    response = client.analyze_document(Document={'Bytes':     image_binary},    #HumanLoopConfig={"FlowDefinitionArn":"arn:aws:sagemaker:us-east-2:572037570099:flow-definition/bs-workflow","HumanLoopName":"bs-workflow",
                          #"DataAttributes" : {ContentClassifiers=["FreeOfPersonallyIdentifiableInformation"|"FreeOfAdultContent",]} },
        FeatureTypes=["TABLES", "FORMS"])
    """
    
    response = client.detect_document_text(Document={'Bytes':     image_binary}   )
    with open('C:/Users/zabarano/Desktop/netandroid/detect_document.json', 'w') as outfile:
        json.dump(response, outfile)
    
    """
    # Alternatively, process using S3 object
    response = client.analyze_document(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}},HumanLoopConfig={"FlowDefinitionArn":"arn:aws:sagemaker:us-east-2:572037570099:flow-definition/bs-workflow",
                          "HumanLoopName":"bs-workflow",
                          #"DataAttributes" : {ContentClassifiers=["FreeOfPersonallyIdentifiableInformation"|"FreeOfAdultContent",]}
                         },
        FeatureTypes=["TABLES", "FORMS"])
    """
    
    #Get the text blocks
    blocks=response['Blocks']
    #width, height =image.size  
    #draw = ImageDraw.Draw(image)  
    print ('*********Detected Document Text***********')
      
    for counter, block in enumerate(blocks):
        if block['BlockType'] == 'LINE':
            DisplayBlockInformation(block, counter)       
        
    return len(blocks)


def detect_image(image_path):

    with open(image_path, 'rb') as srcfile:
        stream = srcfile.read()
    image_bytearray = bytearray(stream)
    """
    srcfile = open(image_path,"rb")
    stream = io.BytesIO(srcfile.read())	
	image_bytearray = stream.getvalue()
    """
    client = boto3.client('textract',region_name='us-east-2',aws_access_key_id='XX',aws_secret_access_key='XXXX')
    response = client.detect_document_text(Document={'Bytes':     image_bytearray}   )
    with open(os.path.splitext(image_path)[0]+ '.json', 'w') as jsonfile:
        json.dump(response, jsonfile)
    return response
	
	
	
	
def detect_classify_img(image_path):	
    response = detect_image(image_path)
    blocks=response['Blocks']
	#filtered_blocks
    lines = "**** LINES *****\n"
    classification = "**** CLASSIFICATION ***** \nMisc\n"
    key_value = "**** KEY VALUE PAIRS ****\n"
    for counter, block in enumerate(blocks):
        if block['BlockType'] == 'LINE':
            lines = lines + DisplayBlockInformation(block, counter)		
	#"blue" followed by "of california"	and contains "Authorization Request"	
    #"Prior Authorization Request Form"
            if re.search("Prior Authorization Request Form", block['Text'], re.IGNORECASE):
                classification = "**** CLASSIFICATION ***** \n Authorization Form !!!\n"			
    #ICD-10 (ICD-10 PRIMARY DX CODE:, ICD-10 ADDITIONAL DX CODE(S):,CPT/HCPCS CODE(S): )
            if re.search("ICD-", block['Text'], re.IGNORECASE):
                key_value = key_value + block['Text'] + "\n"  
            if re.search("CPT/HCPCS", block['Text'], re.IGNORECASE):
                key_value = key_value + block['Text'] + "\n"  				
	#Patient's Name:
            if re.search("Patient", block['Text'], re.IGNORECASE):
                key_value = key_value + block['Text'] + "\n"       	
    #NPJ:	
    #Birth Date
    #Provider information	
    #Pailent information	
    #Servicing Provider/Vendor/Lgb's Name and Address:	
    #Tax ID Number	
    #Referring/Prescribing Physician's Name:
    #Blue Shield ID Number:    
    #Servicing Fagility Name and Address:
    #Place of Service:
    #Anticipated Date of Service:

    with open(os.path.splitext(image_path)[0]+ '.txt', 'wt', encoding="utf-8") as txtfile:
        txtfile.write(classification+lines+key_value)
    txtfile.close()   	
	
	
	
	
	
	
	
	
def split_convert_classify(src_path, out_path):
    pdf_files = (glob.glob(src_path+"/*.pdf"))
    #pdf_files = [os.path.join(src_path, file) for file in os.listdir(src_path) if file.endswith(".pdf")]
    #print(pdf_files)	   
    for pdf_file in pdf_files:
        file_name = Path(pdf_file).stem
       	images = convert_from_path(pdf_file)
        for page,img in enumerate(images): 
            if not os.path.exists(out_path+"/"+file_name):
                print("making dir: "+out_path+"/"+file_name)
                os.makedirs(out_path+"/"+file_name)		
            image_path = out_path+"/"+file_name+"/"+file_name+"_"+str(page+1)+".jpg"				
            print("   saving image: "+image_path)	
            img.save(image_path, 'JPEG')		
            detect_classify_img(image_path)
		
		
	
def main():
    start_time = time.time()

	
	#detect_document_text() is a synchronous API that only support PNG or JPG images. If you'd like to process PDF files, you should use the asynchronous API called start_document_text_detection().
	
    src_path = "C:/Users/zabarano/Desktop/netandroid/classify/src"
    out_path = "C:/Users/zabarano/Desktop/netandroid/classify/out"
    split_convert_classify(src_path, out_path)	
	
    #document = "C:/Users/zabarano/Desktop/netandroid/in.jpg"
    #document = "C:/Users/zabarano/Desktop/netandroid/in.pdf"
    #block_count=detect_document(document)
    #block_count=analyze_document(document)
    #print("Blocks detected: " + str(block_count) +" in " +str(time.time() - start_time) + " seconds")
    
if __name__ == "__main__":
    main()
