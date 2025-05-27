def convert_units(meters):
    centimeters = meters * 100
    feet = meters * 3.28084
    inches = meters * 39.3701

    print(f"\nConversion for {meters} meter(s):")
    print(f"Centimeters: {centimeters:.2f} cm")
    print(f"Feet: {feet:.2f} ft")
    print(f"Inches: {inches:.2f} in")

meters = float(input("Enter length in meters: "))
convert_units(meters)