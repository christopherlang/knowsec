from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker


Base = declarative_base()

engine = create_engine('sqlite:///:memory:', echo=True)


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    password = Column(String)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Adding new row(s) ===========================================================
ed_user = User(name='ed', fullname='Ed Jones', password='edspassword')
session.add(ed_user)

our_user = session.query(User).filter_by(name='ed').first()

ed_user is our_user  # This is extremely useful

session.add_all([
    User(name='wendy', fullname='Wendy Williams', password='foobar'),
    User(name='mary', fullname='Mary Contrary', password='xxg527'),
    User(name='fred', fullname='Fred Flinstone', password='blah')])

ed_user.password = 'f8s7ccs'
session.dirty  # session noticed above change. use len(session.dirty) to see
session.new  # same, but with add_all above. len(session.new) to see

session.commit()  # after this, len on dirty, new should be zero

# Rolling back changes ========================================================
ed_user.name = 'Edwardo'

# Errorneous new user
fake_user = User(name='fakeuser', fullname='Invalid', password='12345')
session.add(fake_user)

# Querying will flush the changes
session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all()

session.rollback()

ed_user.name
fake_user in session
