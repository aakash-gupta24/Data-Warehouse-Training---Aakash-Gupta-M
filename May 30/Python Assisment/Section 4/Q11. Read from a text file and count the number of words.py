with open("Para.txt", "r") as f:
    wc=0
    data = f.readlines()
    for x in data:
        x=x.strip("\n").split(" ")
        wc+=len(x)
    print(f"There are {wc} words in the Para.txt file")
