import requests

def send_log(service_type, application_name, project_name, project_sub_name, azure_hosting_name, 
             developmental_language, description, created_by,log_print, running_within_minutes, error_id):
    
    url = "https://dataserviceheartbeat.azurewebsites.net/api/Is-Run-Log"

    data = {
        "ServiceType": service_type,
        "ApplicationName": application_name,
        "projectName": project_name,
        "projectSubname": project_sub_name,
        "AzureHostingName": azure_hosting_name,
        "DevelopmentalLanguage": developmental_language,
        "Description": description,
        "CreatedBy": created_by,
        "LogPrint": log_print,
        "RunningWithinMinutes": running_within_minutes,
        "ErrorId": error_id
    }
    response = requests.post(url, json=data)
    return response.json()