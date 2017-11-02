import json

Result = open(r"C:\Users\kusha\Downloads\BigData\Python\Demo\Filter.json","w")

states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
with open(r"C:\Users\kusha\Downloads\BigData\Project\yelp\yelp_academic_dataset_business.json", encoding="utf8") as f:
    for line in f:
        data = json.loads(line)
        for st in states:
            if st in data.get("state"):
                try:
                    for i in range(len(data.get("categories"))):
                        if data.get("categories")[i] == "Gyms":
                            Result.write(line)

                except:
                    print("Error")
print(len(data))
Result.close()



