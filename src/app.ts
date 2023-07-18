import express from 'express';
import { MysqlError, createConnection } from 'mysql';

const connection = createConnection({
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'Bittu0808',
    database: 'Bitespeed',
  });
  
connection.connect((err: any) => {
    if (err) {
        console.error('Error connecting to the database:', err);
        return;
    }
    console.log('Connected to the MySQL database');
});

const app = express();

app.get('/', (req: any, res: { send: (arg0: string) => void; }) => {
    res.send('Hello, World!');
});
app.use(express.json());

app.post('/identify', (req, res)=>{
    const {email, phoneNumber} = req.body;
    const query = `SELECT * FROM Contact WHERE email='${email}' OR phoneNumber=${phoneNumber}`
    const resultQuery = `SELECT c1.id AS primaryContactId, JSON_ARRAYAGG(c1.email) AS emails, JSON_ARRAYAGG(c1.phoneNumber) AS phoneNumbers, JSON_ARRAYAGG(c2.id) AS secondaryContactIds FROM  Contact c1 LEFT JOIN  Contact c2 ON c2.linkedId = c1.id WHERE c1.linkPrecedence = "primary" AND (c1.email = '${email}' or c1.phoneNumber = ${phoneNumber}) GROUP BY c1.id;`
    let identity;
    connection.query(query, async(err: any, results: any) => {
        if (err) {
            console.error('Error executing the query:', err);
        } else if(results.length != 0){
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence, linkedId) VALUES ('${email}', ${phoneNumber}, 'secondary', ${results[0].id})`)
        } else{
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence) VALUES ('${email}', ${phoneNumber}, 'primary')`)
        }
    });
    function getIdentity(callback: any) {
        connection.query(resultQuery, (error, response) => {
            if (error) {
                console.error('Error executing the query:', error);
                callback(error, null);
            } else {
                callback(null, response);
            }
        });
    }
    
    getIdentity((error: any, identity: any) => {
        if (error) {
            console.error('Error retrieving identity:', error);
        } else {
            console.log(identity);
            res.json({'contact':identity});
        }
    });    
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
