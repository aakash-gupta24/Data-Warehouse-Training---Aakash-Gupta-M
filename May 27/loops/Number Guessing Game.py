import  random
print("\npress -1 anytime to give up")
gen_num=random.randint(0,100)
guess=-2
times=0
while(guess!=-1):
    guess=int(input("Guess the number :"))
    if (guess==-1):
        break
    times += 1
    if(guess==gen_num):
        print(f"You got it!!! on {times} time")
        break
    elif(guess>gen_num):
        print("Too high")
    elif(guess<gen_num):
        print("Too Low")

if(guess==-1):
    print(f"You gave up after {times} attempt and the number was {gen_num}")