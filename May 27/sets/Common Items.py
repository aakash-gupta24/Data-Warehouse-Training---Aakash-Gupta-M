list1 = [1, 2, 3, 4, 5]
list2 = [4, 5, 6, 7, 8]

common = list(set(list1) & set(list2))

print("Common elements between sets:", common)
