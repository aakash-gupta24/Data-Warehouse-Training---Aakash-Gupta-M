words = []
n=int(input("Enter number of words :"))
for x in range(n):
    words.append(input(f"Enter the word{x+1} :"))

grouped = {}

for word in words:
    length = len(word)
    if length not in grouped:
        grouped[length] = []
    grouped[length].append(word)

print("\nlength-","words\n")
for x in grouped:
    print(x,"-",grouped[x])
