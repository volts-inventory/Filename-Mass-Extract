import glob
import os
import shutil
import multiprocessing
import vertexai
from vertexai.generative_models import GenerativeModel
import pdfplumber

# Manager to create shared object.
manager = multiprocessing.Manager()

# Create a global variable.
dictionary = manager.dict()



def get_chat_response(prompt, chat):
    text_response = []
    responses = chat.send_message(prompt, stream=True)
    for chunk in responses:
        text_response.append(chunk.text)
    return "".join(text_response)

def get_gemini(clean_file, txt):
     # Do cool stuff
    if "conn" not in dictionary:
        model = GenerativeModel("gemini-1.0-pro")
        dictionary["conn"] = model.start_chat(response_validation=False)
    got_name = False
    while not got_name:
        try:
            #name = get_chat_response(f"Please only respond with a single-descriptive title (in the EXACT format: \"<last two digits of publish year>_<2 digit month number of publish date> - <Document Type limited to under 3 words> - <Document Name> - <tag describing unique text content limited to under 5 words>\") for the following file text: \"{txt}\".", chat)
            name = get_chat_response(f"Please only respond with a single-descriptive title (in the EXACT format: \"<Document Name> - <3-5 words detailing unique summaries or conclusions mentioned> - <date in the form YY_MM>\") for the following file text: \"{txt}\".", dictionary["conn"])
            got_name = True
        except ValueError as e:
            #print(f"{clean_file} naughty content...")
            try:
                txt = get_chat_response(f"Please only respond with sanitized text for the following \"{txt}\"", dictionary["conn"])
            except Exception as e:
                print(f"Skipping {clean_file} - bad content")
                return "Unclassifiable"
        except Exception as e:
            print(f"Skipping {clean_file} - bad content.....Retying conn")
            dictionary["conn"] = model.start_chat(response_validation=False)
            return "Unclassifiable"
    # Write to stdout or logfile, etc.
    return name


def process_pdf(file):
    try:
        clean_file = file.split("/")[-1].split(".")[0]
        pdf = pdfplumber.open(file)
        txt = ""
        pages_to_send = 6
        for i in range(0, len(pdf.pages)):
            txt += pdf.pages[i].extract_text() +" "
            if i == pages_to_send:
                break
        name = get_gemini(clean_file, txt)
        name = name.strip().title()
        name =  name.replace("/", " ")
        name =  name.replace("\n", " ")
        name = name[:120]
        print(f"{name}")
        #print(f"Creating: {name} \n \t from {clean_file}. Sent {len(txt)} chars to Gemini")
        shutil.copy(f"./dataset/{clean_file}.pdf", f"./final/{name}.pdf")  
    except Exception as e:
        print(e)

if __name__ == '__main__':
    try:
        shutil.rmtree("./final/")
    except Exception as e:
        print("Fresh Start")
    os.mkdir("./final/") 

    project_id = "declassified-analyzer"
    location = "us-central1"
    vertexai.init(project=project_id, location=location)
    count = int(multiprocessing.cpu_count()/4)
    print(f"Using {count} cores...")
    pool = multiprocessing.Pool(processes=count)
    lst  = glob.glob("./dataset/*.pdf")
    lst.sort(key=lambda x: os.path.getsize(x), reverse=True)
    pool.map(process_pdf, lst)
    
    