string=input("Enter your string :").strip("").lower()
count=0
for x in string:
    if(x=='a'):
        count+=1
print(f"'{string}' has {count} 'a'")