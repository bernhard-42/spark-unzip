import dicttoxml
from faker import Faker

fake = Faker("de_DE")

def createEntry():
    last_name = fake.last_name()
    first_name = fake.first_name()
    city = fake.city()
    country = "Deutschland"
    street = fake.street_name()
    record = {"name":{"first_name":first_name, "last_name":last_name}, 
              "address":{"street":street, "city":city, "country":country}}
    return dicttoxml.dicttoxml(record, attr_type=False)

for f in range(9):
    print f
    records = []
    for r in range(10000):
        records.append(createEntry())
    with open("raw/logfile_%d" % f, "w") as fd:
        fd.write("\n".join(records))

