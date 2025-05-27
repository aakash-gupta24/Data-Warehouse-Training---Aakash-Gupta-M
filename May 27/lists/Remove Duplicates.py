import random

list_of_numbers=[random.randint(0,10) for x in range(random.randint(5,20))]

new_list=[]
for x in list_of_numbers:
    if (x not in new_list):
        new_list.append(x)


print("\n\nElements are generated randomly so run again  if both are same")
print(f"list with duplicate ele (original list):\n{list_of_numbers}")
print(f"list with same order but no duplicates :\n{new_list}")