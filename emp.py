from neo4j import GraphDatabase

# Sample employee data (simulating database table)
# Format: (employee_id, name, title, supervisor_id)
employee_data = [
    (1, "Alice Smith", "CEO", None),
    (2, "Bob Johnson", "CTO", 1),
    (3, "Carol Williams", "CFO", 1),
    (4, "David Brown", "VP Engineering", 2),
    (5, "Emma Davis", "VP Finance", 3),
    (6, "Frank Wilson", "Engineer", 4),
    (7, "Grace Lee", "Engineer", 4),
    (8, "Henry Clark", "Accountant", 5),
    (9, "Isabel Lewis", "Engineer", 4),
    (10, "James Taylor", "Analyst", 5),
]

# Neo4j connection details
NEO4J_URI = "neo4j://localhost:7687"  # Update with your Neo4j URI
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password_here"  # Update with your Neo4j password

class EmployeeHierarchyDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def clear_database(self):
        """Clear all nodes and relationships in the database."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def load_data(self, employee_data):
        """Load employee data into Neo4j as nodes and relationships."""
        with self.driver.session() as session:
            # Create employee nodes
            for emp_id, name, title, _ in employee_data:
                session.run(
                    """
                    CREATE (:Employee {
                        employee_id: $emp_id,
                        name: $name,
                        title: $title
                    })
                    """,
                    emp_id=emp_id, name=name, title=title
                )
            
            # Create SUPERVISES relationships
            for emp_id, _, _, sup_id in employee_data:
                if sup_id is not None:
                    session.run(
                        """
                        MATCH (e:Employee {employee_id: $emp_id}),
                              (s:Employee {employee_id: $sup_id})
                        CREATE (s)-[:SUPERVISES]->(e)
                        """,
                        emp_id=emp_id, sup_id=sup_id
                    )

    def get_employee_and_supervisors(self, employee_id):
        """
        Query the graph to get an employee and all higher-ranking employees above them.
        Returns a list of dictionaries with employee details, sorted by rank hierarchy.
        """
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH path = (root:Employee)-[:SUPERVISES*0..]->(target:Employee {employee_id: $emp_id})
                WHERE NOT ()-[:SUPERVISES]->(root)
                RETURN target.employee_id AS employee_id,
                       target.name AS name,
                       target.title AS title,
                       size([(target)-[:SUPERVISES]->(child) | child]) AS direct_reports,
                       size([(target)-[:SUPERVISES*1..]->(descendant) | descendant]) AS rollups,
                       length(path) AS level
                ORDER BY length(path) DESC
                """,
                emp_id=employee_id
            )
            return [
                {
                    "employee_id": record["employee_id"],
                    "name": record["name"],
                    "title": record["title"],
                    "direct_reports": record["direct_reports"],
                    "rollups": record["rollups"]
                }
                for record in result
            ]

# Example usage
if __name__ == "__main__":
    # Initialize database connection
    db = EmployeeHierarchyDB(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Clear existing data (optional, for clean slate)
        db.clear_database()
        
        # Load employee data
        db.load_data(employee_data)
        
        # Query for an employee and their supervisors
        employee_id = 6  # Example: Frank Wilson (Engineer)
        result = db.get_employee_and_supervisors(employee_id)
        
        if not result:
            print(f"Employee ID {employee_id} not found in the graph.")
        else:
            print(f"Employee and supervisors for ID {employee_id} (in rank hierarchy):")
            for emp in result:
                print(f"ID: {emp['employee_id']}, Name: {emp['name']}, Title: {emp['title']}, "
                      f"Direct Reports: {emp['direct_reports']}, Rollups: {emp['rollups']}")
    
    finally:
        # Close the database connection
        db.close()