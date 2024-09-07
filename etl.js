import fs from 'fs';
import csv from 'csv-parser';

const amexRowBuilder = (row) => {
    return ['Amex', row['Date'], `"${row['Description']}"`, row['Amount']];
}
const chaseRowBuilder = (row) => {
    return ['Southwest', row['Post Date'], `"${row['Description']}"`, row['Amount']];
}
const checkingRowBuilder = (row) => {
    return ['Checking', row['Posted Date'], `"${row['Description']}"`, row['Amount']];
}

export class BankActivityEtl {
    headers = [];
    numRows = 0;
    writeStream = null;
    builder = () => { };

    constructor(inputFileName, outputFileName) {
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
    }

    _getBuilder(headers) {
        if (headers.includes('post date')) {
            return chaseRowBuilder;
        }
        if (headers.includes('posted date')) {
            return checkingRowBuilder;
        }
        if (headers.includes('date')) {
            return amexRowBuilder;
        }
        throw new Error('The date header was not recognized.');
    }

    _stringifyRow(row) {
        return row.join(',') + '\n';
    }

    _onData(row) {
        if (this.headers.length === 0) {
            console.log(`Successfully opened ${this.inputFileName} was successfully opened.`);
            this.headers = Object.keys(row).map((key) => key.toLowerCase().trim());
            this.builder = this._getBuilder(this.headers);
            this.writeStream.write(this._stringifyRow(['Source', 'Date', 'Description', 'Amount']));
        }

        this.numRows++;
        this.writeStream.write(this._stringifyRow(this.builder(row)));
    }

    _onEnd() {
        console.log(this.numRows, 'rows processed')
        console.log('CSV processing complete');
        this.writeStream.end();
    }

    process() {
        this.writeStream = fs.createWriteStream(this.outputFileName);
        fs.createReadStream(this.inputFileName)
            .pipe(csv())
            .on('data', this._onData.bind(this))
            .on('end', this._onEnd.bind(this));

    }
}