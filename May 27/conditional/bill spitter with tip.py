
bill_amount = float(input("Enter total bill amount:"))
num_people = int(input("Enter number of people: "))
tip_percent = float(input("Enter tip percentage: "))

if bill_amount < 0 or num_people <= 0 or tip_percent < 0:
    print("Please enter valid positive values.")

tip_amount = (tip_percent / 100) * bill_amount
total_with_tip = bill_amount + tip_amount
amount_per_person = total_with_tip / num_people
print("\n--- Bill Summary ---")
print(f"Total bill with tip: ${total_with_tip:.2f}")
print(f"Each person should pay: ${amount_per_person:.2f}")
