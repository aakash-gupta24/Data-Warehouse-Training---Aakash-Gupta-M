class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def description(self):
        print(f"Vehicle Name: {self.name}")
        print(f"Wheels: {self.wheels}")

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type

    def description(self):
        print(f"Car Name: {self.name}")
        print(f"Wheels: {self.wheels}")
        print(f"Fuel Type: {self.fuel_type}")

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared

    def description(self):
        print(f"Bike Name: {self.name}")
        print(f"Wheels: {self.wheels}")
        print(f"Geared: {'Yes' if self.is_geared else 'No'}")


car = Car("Sedan", 4, "Petrol")
bike = Bike("Mountain Bike", 2, True)

car.description()
print()
bike.description()
