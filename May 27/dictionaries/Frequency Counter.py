text = "hello world"

freq = {}

for char in text:
    if char in freq:
        freq[char] += 1
    else:
        freq[char] = 1
print("char","->","freq")
for x in freq:
    print(x,"->",freq[x])
