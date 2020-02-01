import re
import ast

# converts the header string from web devtools to a dict for requests
def headersToDict(headers):
    headers = headers + "\n"
    headers = re.sub(r'(.*?):[\s]*(.*?)\n',r'"\1":"\2",',headers).strip(",") #(^[\S].*?):[\s]*(.*?)\n
    headers = "{"+headers+"}"
    headerDict = ast.literal_eval(headers)
    return headerDict
