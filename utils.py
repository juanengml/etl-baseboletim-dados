import re

regex_list = {
  "dmyyyy": r"([0-9]{1})([0-9]{2})([0-9]{4})",
  "ddmmyyyy":r"([0-9]{2})([0-9]{2})([0-9]{4})"
}

def format_data(padrao,dados):
  padroes = list(regex_list.keys())
  data = ""
  if padrao == padroes[0]: #  "dmyyyy"
    dia, mes, ano = dados 
    data = "{}/{}/{}".format(dia,mes,ano)  
     
  if padrao == padroes[1]: #  "ddmmyyyy"
    dia, mes, ano = dados 
    data = "{}/{}/{}".format(dia,mes,ano)  
  return data   
    
def checking_valid_dates(dt):
    result = {"padrao":0,"result":0}
    dt = str(dt)
    for regex_dt in list(regex_list.keys()):
      r = re.findall(regex_list[regex_dt], dt)
      #print(r)
      if len(r) == 0: pass
      else:
        result['result'] = list(r)
        result['padrao'] = regex_dt
        print(result)
    if len(result['result']) == 0:
        return dt
    else:
        return  format_data(result['padrao'],list(result['result'][0]))
