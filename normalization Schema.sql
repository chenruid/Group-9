CREATE TABLE Offices (
    OfficeID INT PRIMARY KEY,
    Address VARCHAR(255),
    City VARCHAR(100),
    State VARCHAR(100),
    ZipCode VARCHAR(20),
    Phone VARCHAR(20),
    Income  VARCHAR(20)
);

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    OfficeID INT,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Email VARCHAR(100),
    Position VARCHAR(100),
    EmploymentType VARCHAR(50),
    FOREIGN KEY (OfficeID) REFERENCES Offices(OfficeID),
    Salaries  VARCHAR(20)
);

CREATE TABLE Expenses (
    ExpenseID INT PRIMARY KEY,
    OfficeID INT,
    ExpenseType VARCHAR(100),
    Amount DECIMAL(10, 2),
    ExpenseDate DATE,
    FOREIGN KEY (OfficeID) REFERENCES Offices(OfficeID)
);

CREATE TABLE Managing (
    ManagingID INT PRIMARY KEY,
    EmployeeID INT,
    HomeID INT,
    ClientID INT,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (HomeID) REFERENCES Homes(HomeID),
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID)
);

CREATE TABLE Homes (
    HomeID INT AUTO_INCREMENT PRIMARY KEY,
    Type INT,
    Address VARCHAR(255) NOT NULL,
    City VARCHAR(100) NOT NULL,
    State VARCHAR(100) NOT NULL,
    ZipCode VARCHAR(10) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    Status INT,
    FOREIGN KEY (Type) REFERENCES HomeTypes(TypeID),
    FOREIGN KEY (Status) REFERENCES Availability(StatusID)
);

CREATE TABLE HomeTypes (
    TypeID INT AUTO_INCREMENT PRIMARY KEY,
    TypeName VARCHAR(255) NOT NULL
);

CREATE TABLE Features (
    FeatureID INT AUTO_INCREMENT PRIMARY KEY,
    FeatureName VARCHAR(255) NOT NULL
);

CREATE TABLE Availability (
    StatusID INT AUTO_INCREMENT PRIMARY KEY,
    StatusName VARCHAR(255) NOT NULL
);
CREATE TABLE Home_Features (
    HomeID INT,
    FeatureID INT,
    FOREIGN KEY (HomeID) REFERENCES Homes(HomeID),
    FOREIGN KEY (FeatureID) REFERENCES Features(FeatureID),
    PRIMARY KEY (HomeID, FeatureID)
);

CREATE TABLE Clients (
    ClientID INT AUTO_INCREMENT PRIMARY KEY,
    FirstName VARCHAR(255) NOT NULL,
    LastName VARCHAR(255) NOT NULL,
    Email VARCHAR(255) NOT NULL,
    Phone VARCHAR(20),
    ContactPreference VARCHAR(50) NOT NULL
);
CREATE TABLE Client_Preferences (
    PreferenceID INT AUTO_INCREMENT PRIMARY KEY,
    ClientID INT,
    TypeID INT,
    MinPrice DECIMAL(10, 2),
    MaxPrice DECIMAL(10, 2),
    Features VARCHAR(255),
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (TypeID) REFERENCES HomeTypes(TypeID)
);
CREATE TABLE Transactions (
    TransactionID INT AUTO_INCREMENT PRIMARY KEY,
    ClientID INT,
    HomeID INT,
    TransactionType VARCHAR(50) NOT NULL,
    TransactionDate DATE NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (HomeID) REFERENCES Homes(HomeID)
);
CREATE TABLE Client_Agent (
    ClientID INT,
    AgentID INT,
    AssignmentDate DATE NOT NULL,
    PRIMARY KEY (ClientID, AgentID),
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (AgentID) REFERENCES Managing(EmployeeID)
);

CREATE TABLE Appointments (
    AppointmentID INT AUTO_INCREMENT PRIMARY KEY,
    ClientID INT,
    AgentID INT,
    HomeID INT,
    AppointmentDate DATETIME NOT NULL,
    Purpose VARCHAR(255),
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (AgentID) REFERENCES Managing(EmployeeID),
    FOREIGN KEY (HomeID) REFERENCES Homes(HomeID)
);

CREATE TABLE Open_Houses (
    OpenHouseID INT AUTO_INCREMENT PRIMARY KEY,
    HomeID INT,
    AgentID INT,
    OpenHouseDate DATETIME NOT NULL,
    Duration INT NOT NULL,
    VisitorCount INT NOT NULL,
    FOREIGN KEY (HomeID) REFERENCES Homes(HomeID),
    FOREIGN KEY (AgentID) REFERENCES Managing(EmployeeID)
);

CREATE TABLE Services (
    ServiceID INT PRIMARY KEY,
    ServiceName VARCHAR(255),
    Description TEXT
);

CREATE TABLE Service_Appointments (
    ServiceAppointmentID INT PRIMARY KEY,
    ClientID INT,
    ServiceID INT,
    AppointmentDate DATETIME,
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (ServiceID) REFERENCES Services(ServiceID)
);

