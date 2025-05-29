def convert_temp(value, unit):
    if unit == 'C':
        return (value * 9 / 5) + 32
    elif unit == 'F':
        return (value - 32) * 5 / 9
    else:
        raise ValueError("Invalid unit. Use 'C' for Celsius or 'F' for Fahrenheit.")


val=int(input("Enter the value:"))
unit=input("Enter the unit (C/F):").upper()
if(unit=="C"):
    print(f"{val}C is equal to {convert_temp(val,unit)}F")
else:
    print(f"{val}F is equal to {convert_temp(val, unit)}C")