class Person:
  def __init__(self, fname, lname):
    self.firstname = fname
    self.lastname = lname

  def printname(self):
    print(self.firstname, self.lastname)

  def makename(self):
    return self.firstname + ", " + self.lastname


x = Person("Emma", "Haddi")
print(x.makename())


class Student(Person):
  pass

y = Student("Sanika", "Joshi")
print(y.makename())