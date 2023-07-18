import express from 'express';
import { MysqlError, createConnection } from 'mysql';

const connection = createConnection({
    host: 'localhost',
    port: 3306,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DB,
  });
  
connection.connect((err: any) => {
    if (err) {
        console.error('Error connecting to the database:', err);
        return;
    }
    console.log('Connected to the MySQL database');
});

const processQueryResult = (queryResult:any) => {
    const {primaryContactId, emails, phoneNumbers, secondaryContactIds} = queryResult;
    
    const emailResult: string[] = [];
    for (let email of JSON.parse(emails) as string[]){        
        if (!emailResult.includes(email)){
            emailResult.push(email)
        }
    }
    const phoneNumberResult: number[] = [];
    for (let phoneNumber of JSON.parse(phoneNumbers) as number[]){
        if (!phoneNumberResult.includes(phoneNumber)){
            phoneNumberResult.push(phoneNumber)
        }
    }
    return {
        'primaryContactId': primaryContactId,
        'emails': emailResult,
        'phoneNumbers': phoneNumberResult,
        'secondaryContactIds': JSON.parse(secondaryContactIds) as number[]
    }
}



const app = express();

app.get('/', (req: any, res: { send: (arg0: string) => void; }) => {
    res.send('Hello, World!');
});
app.use(express.json());

app.post('/identify', (req, res)=>{
    const {email, phoneNumber} = req.body;
    const query = `SELECT * FROM Contact WHERE email='${email}' OR phoneNumber=${phoneNumber} ORDER BY id`
    const resultQuery = `SELECT 
        c1.id AS primaryContactId, 
        JSON_ARRAYAGG(c2.email) AS emails, 
        JSON_ARRAYAGG(c2.phoneNumber) AS phoneNumbers, 
        JSON_ARRAYAGG(c2.id) AS secondaryContactIds 
    FROM  Contact c1 LEFT JOIN  Contact c2 ON (c2.linkedId = c1.id or c2.id = c1.id) 
    WHERE c1.email = '${email}' or c1.phoneNumber = ${phoneNumber} 
    GROUP BY c1.id;`
    
    const getIdentity = (callback: any) => {
        connection.query(resultQuery, (error, response) => {
            console.log('Result Query start');
            if (error) {
                console.error('Error executing the query:', error);
                callback(error, null);
            } else {
                callback(null, response);
            }
            console.log('Result Query end');
        });
    };

    const getCountToUpdate = (callback: any) => {
        connection.query(`SELECT COUNT(*) - 2 FROM Contact WHERE email = '${email}' OR phoneNumber = ${phoneNumber}`, (count: any) => {
            console.log('Count Query start', count);
            callback(count);
        });
    };

    connection.query(query, (err: any, results: any) => {
        console.log('Insert Query start');
        if (err) {
            console.error('Error executing the query:', err);
        } else if(results.length == 0){
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence) VALUES ('${email}', ${phoneNumber}, 'primary')`)
        } else {
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence, linkedId) VALUES ('${email}', ${phoneNumber}, 'secondary', ${results[0].linkPrecedence == 'primary' ? results[0].id : results[0].linkedId})`)
        }
        console.log('Insert Query end');
    });

    getCountToUpdate((count: any)=>{
        if (count != null){
            connection.query(`UPDATE Contact
            SET linkedId = (
                SELECT linkedId
                FROM (
                    SELECT DISTINCT c1.linkedId AS linkedId
                    FROM Contact c1
                    WHERE c1.email = '${email}' OR c1.phoneNumber = ${phoneNumber}
                    ORDER BY c1.linkedId
                    LIMIT 1 OFFSET 1
                ) AS subquery
            )
            WHERE (email = '${email}' OR phoneNumber = ${phoneNumber})
            AND linkedId IN (
                SELECT linkedIds
                FROM (
                    SELECT DISTINCT c1.linkedId AS linkedIds
                    FROM Contact c1
                    WHERE c1.email = '${email}' OR c1.phoneNumber = ${phoneNumber}
                    ORDER BY c1.linkedId
                    LIMIT 2, ${count}
                ) AS subquery) OR (linkPrecedence = 'primary' AND linkedId != (
                SELECT linkedId
                FROM (
                    SELECT DISTINCT c1.linkedId AS linkedId
                    FROM Contact c1
                    WHERE c1.email = '${email}' OR c1.phoneNumber = ${phoneNumber}
                    ORDER BY c1.linkedId
                    LIMIT 1 OFFSET 1
                ) AS subquery
                ))); 
                UPDATE Contact SET linkPrecedence = 'secondary' 
                WHERE email = '${email}' OR phoneNumber = ${phoneNumber}
                AND (linkPrecedence = 'primary' AND linkedId != (
                    SELECT linkedId
                    FROM (
                        SELECT DISTINCT c1.linkedId AS linkedId
                        FROM Contact c1
                        WHERE c1.email = '${email}' OR c1.phoneNumber = ${phoneNumber}
                        ORDER BY c1.linkedId
                        LIMIT 1 OFFSET 1
                    ) AS subquery
                ));
                `, (err: any, results: any) => {
                console.log('Secondary Update Query start');
                if (err) {
                    console.error('Error executing the query:', err);
                    return
                } else {
                    console.log('update query results', results);
                }
                console.log('Secondary Update Query end', count);
            });
        }
        console.log('Count Query end', count);
    });
    


    getIdentity((error: any, identity: any) => {
        if (error) {
            console.error('Error retrieving identity:', error);
        } else {
            res.json({'contact':processQueryResult(identity[0])});
        }
    });    
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
