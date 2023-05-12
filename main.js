const { time } = require('console');

const testDB = require('nano')('http://admin:password@127.0.0.1:5984/test_db');


const defaultOpts = {
    DB: testDB,
    docNum: 1,
    docSize: 1, // in Bytes
    verbose: false,
}


async function insert(option) {
    console.time('insert');
    for (let i = 0; i < option.docNum; i++) {
        const doc = {
            _id: i.toString(),
            val: 'X'.repeat(option.docSize),
        }
        const response = await option.DB.insert(doc)
    }
    console.timeEnd('insert');
}
async function insertBulk(option) {
    if (option.verbose) console.log('insertBulk: ', option);

    const documents = []
    for (let i = 0; i < option.docNum; i++) {
        documents.push({
            _id: i.toString(),
            val: 'X'.repeat(option.docSize),
        })
    }

    const response = await option.DB.bulk({ docs: documents })
    console.log(response)
}
async function delteBulk(option) {
    // if (option.verbose)   console.log('deleteBulk: ', option);

    const keys = []
    for (let i = 0; i < option.docNum; i++) {
        keys.push(i.toString())
    }
    const res = await option.DB.fetch({ keys: keys })
    if (option.verbose) console.log(res.rows);
    const docs = res.rows.filter(row => row.doc).map(row => {
        return {
            '_id': row.doc._id,
            '_rev': row.doc._rev,
            '_deleted': true,
        }
    })

    const response = await option.DB.bulk({ docs: docs })
    console.log(response)
}

// delteBulk({
//     ...defaultOpts,
//     // verbose: true,
//     docNum: 1000,
// })




insert({
    ...defaultOpts,
    docNum: 100,
    docSize: 1000,
})
// insertBulk({
//     ...defaultOpts,
//     docNum: 1000,
//     docSize: 1000,
// })
