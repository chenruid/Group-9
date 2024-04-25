# Query 1: Employee Performance Overview
# This query retrieves information on employees and the number of transactions they have managed, sorted by the number of transactions to identify top performers.

SELECT 
    E.FirstName,
    E.LastName,
    E.Position,
    COUNT(T.TransactionID) AS NumberOfTransactions
FROM 
    Employees E
JOIN 
    Managing M ON E.EmployeeID = M.EmployeeID
JOIN 
    Transactions T ON M.ManagingID = T.HomeID
GROUP BY 
    E.EmployeeID
ORDER BY 
    NumberOfTransactions DESC;

# Query 2: Office Expenses and Revenue Comparison
# This query provides a comparison of income and expenses for each office, giving an overview of the financial health of each location. It uses data from the Offices, Expenses, and an aggregated sum of Transactions to show total revenue per office. This demonstrates the database's ability to handle complex joins, subqueries, and aggregate data effectively.

SELECT 
    O.OfficeID,
    O.Address,
    O.City,
    O.State,
    IFNULL(SUM(T.Price), 0) AS TotalRevenue,  -- Handling possible NULL values with IFNULL
    (
        SELECT SUM(E.Amount)
        FROM Expenses E
        WHERE E.OfficeID = O.OfficeID
    ) AS TotalExpenses
FROM 
    Offices O
LEFT JOIN 
    Employees EMP ON O.OfficeID = EMP.OfficeID  -- Correctly linking Offices to Employees
LEFT JOIN 
    Managing M ON EMP.EmployeeID = M.EmployeeID  -- Linking Managing through Employees
LEFT JOIN 
    Transactions T ON M.HomeID = T.HomeID  -- Correctly linking Transactions to Managing via HomeID
GROUP BY 
    O.OfficeID
ORDER BY 
    O.OfficeID;


# Query 3: List Clients and Their Recent Transactions

SELECT C.FirstName, C.LastName, T.TransactionType, T.TransactionDate, T.Price
FROM Clients C
JOIN Transactions T ON C.ClientID = T.ClientID
WHERE T.TransactionDate > DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY T.TransactionDate DESC;

# Query 4: Employee Details and Their Office Locations
SELECT E.FirstName, E.LastName, E.Position, O.Address
FROM Employees E
JOIN Offices O ON E.OfficeID = O.OfficeID;

# Query 5: Offices with the Highest Expenses
SELECT O.OfficeID, O.Address, SUM(E.Amount) AS TotalExpenses
FROM Expenses E
JOIN Offices O ON E.OfficeID = O.OfficeID
GROUP BY E.OfficeID
ORDER BY TotalExpenses DESC
LIMIT 5;

# Query 6: Homes Listed in the Last Month by Type
SELECT H.Address, H.City, H.State, HT.TypeName
FROM Homes H
JOIN HomeTypes HT ON H.Type = HT.TypeID
WHERE H.Status = 1 AND H.Price > 500000
ORDER BY H.Price DESC;

# Query 7: Average Transaction Price per Home Type
SELECT HT.TypeName, AVG(T.Price) AS AveragePrice
FROM Transactions T
JOIN Homes H ON T.HomeID = H.HomeID
JOIN HomeTypes HT ON H.Type = HT.TypeID
GROUP BY HT.TypeName;

#Query 8: Count of Features per Home
SELECT H.Address, COUNT(HF.FeatureID) AS FeatureCount
FROM Home_Features HF
JOIN Homes H ON HF.HomeID = H.HomeID
GROUP BY H.HomeID;

# Query 9: Total Revenue by Office
SELECT O.Address, SUM(T.Price) AS TotalRevenue
FROM Transactions T
JOIN Managing M ON T.HomeID = M.HomeID
JOIN Employees E ON M.EmployeeID = E.EmployeeID
JOIN Offices O ON E.OfficeID = O.OfficeID
GROUP BY O.OfficeID;

# Query 10: Clients Without Transactions in the Last Year
SELECT C.FirstName, C.LastName, C.Email
FROM Clients C
LEFT JOIN Transactions T ON C.ClientID = T.ClientID AND T.TransactionDate > DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
WHERE T.TransactionID IS NULL;
