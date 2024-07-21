import csv

class SaveResults:

    def __init__(self) -> None:
        pass

    def save_results(self, lib:str, total_records:int, operation_type:str, time_taken:float):
        field_names= ['Library', 'Total_Records', 'Operation_Type', 'Time_Taken']
        row = {
            'Library':lib, 
            'Total_Records':total_records, 
            'Operation_Type':operation_type, 
            'Time_Taken':time_taken
        }
        with open('./Data/result.csv', 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writerow(row)