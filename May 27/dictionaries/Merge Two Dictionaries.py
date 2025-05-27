dict1 = {'a': 10, 'b': 20, 'c': 30}
dict2 = {'b': 5, 'c': 15, 'd': 25}

merged = {}

for key, value in dict1.items():
    merged[key] = value

for key, value in dict2.items():
    if key in merged:
        merged[key] += value
    else:
        merged[key] = value
print(f"dict 1 :{dict1}")
print(f"dict 2 :{dict2}")
print(f"merged dictionary :{merged}")
