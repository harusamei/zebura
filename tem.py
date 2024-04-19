class Animal:
    def __init__(self, name):
        self.name = name

    def speak(self):
        print("The animal makes a sound.")

class Dog(Animal):
    def __init__(self, name, breed):
        super().__init__(name)
        self.breed = breed
        print(self.name)
        
    def speak(self):
        super().speak()
        print("The dog barks.")

class Cat(Animal):
    def __init__(self, name, breed):
        super().__init__(name)
        self.breed = breed

    
# 创建一个Animal实例
animal = Animal("Generic Animal")
animal.speak()

# 创建一个Dog实例
dog = Dog("Buddy", "Golden Retriever")
dog.speak()
cat = Cat("Kitty", "Siamese")
cat.speak()