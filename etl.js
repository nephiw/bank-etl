import fs from 'fs';
import csv from 'csv-parser';
import dayjs from 'dayjs';

const DATE_FORMAT = 'YYYY-MM-DD';

const amexRowBuilder = (row) => {
    // American Express makes debts positive and payments negative.
    const amount = parseFloat(row['Amount'], 10) * -1;
    const date = dayjs(row['Date'], 'MM/DD/YYYY');
    return ['Amex', date.format(DATE_FORMAT), `"${row['Description']}"`, `${amount}`];
}
const chaseRowBuilder = (row) => {
    const date = dayjs(row['Post Date'], 'MM/DD/YYYY');
    return ['Southwest', date.format(DATE_FORMAT), `"${row['Description']}"`, row['Amount']];
}
const checkingRowBuilder = (row) => {
    const date = dayjs(row['Posted Date'], 'YYYY-MM-DD');
    return ['Checking', date.format(DATE_FORMAT), `"${row['Description']}"`, row['Amount']];
}

export class BankActivityEtl {
    headers = [];
    numRows = 0;
    writeStream = null;
    append = false;
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
            if (!this.append) {
                this.writeStream.write(this._stringifyRow(['Source', 'Date', 'Description', 'Amount']));
            }
        }

        this.numRows++;
        this.writeStream.write(this._stringifyRow(this.builder(row)));
    }

    _onEnd() {
        console.log(this.numRows, 'rows processed')
        console.log('CSV processing complete');
        this.writeStream.end();
    }

    process(append) {
        this.append = append;
        this.writeStream = fs.createWriteStream(this.outputFileName, { flags: append ? 'a' : 'w' });
        fs.createReadStream(this.inputFileName)
            .pipe(csv())
            .on('data', this._onData.bind(this))
            .on('end', this._onEnd.bind(this));

    }
}
