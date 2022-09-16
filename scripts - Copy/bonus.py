import requests
import os
import logzero
from logzero import logger
from bonusfunctions import * 
from calendar import isleap
import pandas as pd
from dateutil.relativedelta import relativedelta
from time import sleep
import shutil
from operator import itemgetter


def get_bonus():
        notifications=""
        Workday_String_date_Formatter = "%Y-%m-%d"
        quarters =[]
        url="<ul>"
        try:
                logger.info("Reading Parameters")
                XLS = read_xls_parameters()
                if type(XLS) == str:
                        return f"{XLS}"
                notifications += "<li>Parameters Received</li><br>"
        except:
                Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                return "Error : Cant get the parameter file  //"
        else:      
                start_date = XLS.iloc[0,1]
                end_date = XLS.iloc[0,2]
                cutoff =  XLS.iloc[0,3] - relativedelta(days=1) 
                company_performances = round(Decimal(XLS.iloc[0,4]),set_ndigits)
                quarter_to_calculate = int(XLS.iloc[0,5])
                Workday_start_date = XLS.iloc[1,2]
                Workday_end_date = XLS.iloc[1,4]
                #Workday_start_date = cutoff - relativedelta(months=18)
                Workday_String_Start_date = Workday_start_date.strftime(Workday_String_date_Formatter)
                Workday_String_End_date = Workday_end_date.strftime(Workday_String_date_Formatter)
                Year_To_calculate = int(start_date.strftime("%Y")) + 1
                String_Cutoff  = cutoff.strftime(Workday_String_date_Formatter)
                #print(Workday_String_Start_date, String_Cutoff)
                quarters = get_quarters(f"{Year_To_calculate}")
                incoming_file_name = f"{ROOT_DIR}\\Data\\Bonus_Parameters.xlsx"
                report_file_name= f"{ROOT_DIR}\\Data\\Bonus_Report_RaaS_{Year_To_calculate}.xlsx"
                shutil.copyfile(incoming_file_name, report_file_name)
                if type(quarters) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{quarters}  // "
                taxes = create_taxes(XLS)
                if type(taxes) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{taxes}  // "
                Days_By_Quarter = [92,92,89,92,365]
                if isleap(Year_To_calculate):
                        Days_By_Quarter = [92,92,90,92,366]
                proration = [float(Days_By_Quarter[0])/Days_By_Quarter[4],float(Days_By_Quarter[1])/Days_By_Quarter[4], float (Days_By_Quarter[2])/Days_By_Quarter[4], float (Days_By_Quarter[3])/Days_By_Quarter[4]]
                url = Create_Workday_Url(Workday_String_Start_date,Workday_String_End_date,"csv")
                logger.info("Downloading csv")
                if "Error" in url:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{url} // "
                notifications = f"<li>Calculation done with {company_performances}  percent of Company Performance </li><br>"
                notifications += "<li>Csv file downloaded</li><br>"
                status_of_data = get_employee_data_file(url,Year_To_calculate,"csv")
                if "ERROR" in status_of_data:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{status_of_data}  //"
                logger.info("Downloading XML")
                notifications += "<li>XML file downloaded</li><br>"
                url = Create_Workday_Url(Workday_String_Start_date,String_Cutoff,"xml")
                if "Error" in url:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{url} // "
                status_of_data = get_employee_data_file(url,Year_To_calculate,"xml")
                if "ERROR" in status_of_data:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{status_of_data}  //"
                logger.info("Transforming")
                #transform(Year_To_calculate)
                logger.info("Getting the Dictionary")
                employee_data = Get_Xml_Data(Year_To_calculate)
                if type(employee_data) is not dict:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{employee_data}  //"
                logger.info("Closing the file")
                Save_Sheet(employee_data,report_file_name,f"Employee_info_from_Workday", 0, 0)
                logger.info("Removing employees without benefits")
                employee_data = get_dict_with_condition(employee_data,"Employee_Status","1")
                if type(employee_data) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{employee_data}  //"
                employee_data = get_dict_with_condition(employee_data,"Employee_Type","Regular")
                if type(employee_data) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{employee_data}  //"
                employee_data = get_data_before_cutoff(employee_data,cutoff)
                if type(employee_data) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{employee_data}  //"
                print(employee_data)
                if type(employee_data) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{employee_data}  //"
                notifications += "<li>Employees with no benefits removed</li><br>"
                #Save_Sheet(employee_data,report_file_name,f"Employees_With_Benefits", 0, 0)
                employee_data = order_dic(employee_data,True)
                logger.info("Creating the pivot")        
                ordered_eligible_employee = employee_data.copy()
                ordered_eligible_employee = order_dic(ordered_eligible_employee,False)
                #with open (f"{ROOT_DIR}\\Data\\edata.txt","w") as nda:
                #          nda.write(f"{employee_data}") 
                Save_Sheet(ordered_eligible_employee,f"{ROOT_DIR}\\Data\\Bonus_Report_RaaS_{Year_To_calculate}.xlsx",f"Eligible Employees", 0, 0)
                pivot_data = create_pivot_data(employee_data,quarters,proration,taxes,cutoff,quarter_to_calculate,company_performances,Year_To_calculate)
                #print(pivot_data.keys())
                ordered_pivot_data = pivot_data.copy()
                ordered_pivot_data = order_pivot_dic(ordered_pivot_data)
                Save_Sheet(ordered_pivot_data,report_file_name,f"Bonus Calc - entity dept period", 0, 0)
                if type(pivot_data) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{pivot_data}" 
                logger.info("Pivot Table Created")
                notifications += "<li>Pivot table created</li><br>"
                pivot = create_pivot(pivot_data)
                #with open(f"{ROOT_DIR}\\Data\\pdata.txt","w") as nda:
                #          nda.write(f"{pivot_data}") 
                #pivot_data = order_dic(pivot_data,False)
                #Save_Sheet(pivot,f"{ROOT_DIR}\\Data\\Bonus_Report_RaaS_{Year_To_calculate}.xlsx",f"Employees With Benefits", 0, 0)
                if type(pivot) == str:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return f"{pivot} // //"
                #with open(f"{ROOT_DIR}\\Data\\pvdata.txt","w") as nda:
                #          nda.write(f"{pivot}")
                Save_Sheet(pivot,report_file_name,f"Pivot - Currency", 0, 0)
                end_date = quarters[quarter_to_calculate-1][1]
                reversal = (end_date + relativedelta(months=3)).strftime("%b-%y")
                notifications += "<li>Reversal month  Setted</li><br>"
                logger.info("Creating journals")
                result =[]
                #print(pivot)
                ######
                result = create_Journals(pivot,end_date,reversal,quarter_to_calculate,taxes,quarters)
                try:            
                    jrnl = result[0]# order_any_dic(result[0],itemgetter("Entity", "Dpt"))
                    jrnl = order_journal_dic(jrnl)
                    tx_jrnl = result[1]#order_any_dic(result[1],itemgetter("Entity", "Dpt"))
                    tx_jrnl = order_journal_dic(tx_jrnl)
                    usd_jrnl = result[2]#order_any_dic(result[2],itemgetter("Entity", "Dpt"))
                    usd_jrnl = order_journal_dic(usd_jrnl)
                    tx_usd_jrnl =  result[3]#order_any_dic(result[3],itemgetter("Entity", "Dpt"))
                    tx_usd_jrnl = order_journal_dic(tx_usd_jrnl)
                    notifications += "<li>Journal Created</li><br>"
                    Save_Sheet(jrnl,report_file_name,f"Journal", 0, 0)
                    Save_Sheet(tx_jrnl,report_file_name,f"Taxes_Journal", 0, 0)
                    Save_Sheet(usd_jrnl,report_file_name,f"Journal_Converted", 0, 0)
                    Save_Sheet(tx_usd_jrnl,report_file_name,f"Taxes_Converted", 0, 0)
                    notifications += "<li>Journals Pushed</li><br>"
                except:
                        return f"Error cant create journals --> {result[0]} // "
                try:
                        logger.info("Trying to post")
                        notifications += "<li>Journal Imported</li><br>"
                        Autopost_payload = Create_AutoPost()
                        response_autopost=AutoPost(Autopost_payload)
                        Autopost_Reqid = json.loads(response_autopost.text)["ReqstId"]
                        while True:
                                Status = json.loads(Get_Status(Autopost_Reqid).text)["items"][0]["RequestStatus"]
                                if Status == "SUCCEEDED":
                                        break
                                if Status == "ERROR" or Status == "WARNING":
                                        logger.info("Posting Error")
                                        return f"Error or warning  on posting // //"
                        print("Posted")
                        print("Finished")
                except:
                        Push_To_S3(f"{logs_path}\\functions.log","Process4","Log")
                        return "Error on posting // //" 
                else:
                        notifications += "<li>Journals Posted</li><br><ul>"
                        with open(f"{config_path}\\config.cfg", "a+") as config:
                                config.write(f"{Year_To_calculate}-{quarter_to_calculate}\n")
                        logger.info("Done")
                        try:
                                with open(f"{logs_path}\\functions.log", "rb") as f:
                                        key = f"{logs_path}\\functions.log".split("\\")[-1]
                                        response = s3_bucket.upload_fileobj(f, bucket_name, f"Process4/Logs/{key}")
                                        f.close()
                        except Exception as e:
                                logger.info(f"Error occurs while uploading")
                return f"{notifications} // {data_path}\\{Year_To_calculate}_bonusfile.csv;{ROOT_DIR}\\Data\\Bonus_Report_RaaS_{Year_To_calculate}.xlsx;{data_path}\\calculation.txt"

        

      
