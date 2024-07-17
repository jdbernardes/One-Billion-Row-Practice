import csv
from faker import Faker

class DataGenerator:
    def __init__(self) -> None:
        pass

    def generate_data(self, num_records:int, locale:str=None) -> list:
        fake = Faker(locale = locale)
        credit_card_dict: dict = {}
        credit_cards: list = []
        for _ in range(num_records):
            credit_card_dict['Card_Number'] = fake.credit_card_number()
            credit_card_dict['Expiry_Date'] = fake.credit_card_expire()
            credit_card_dict['Card_Provider'] = fake.credit_card_provider()
            credit_card_dict['Security_Code'] = fake.credit_card_security_code()
            credit_cards.append(credit_card_dict)
            credit_card_dict = {}
        return credit_cards
    
    def create_csv(self, num_records:int, locale:str=None) -> None:
        field_names= ['Card_Number', 'Expiry_Date', 'Card_Provider', 'Security_Code']
        credit_cards = self.generate_data(num_records, locale)
        with open('./Data/data.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(credit_cards)
        