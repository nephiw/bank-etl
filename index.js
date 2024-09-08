#!/usr/bin/env node
import fs from 'fs';
import readline from 'node:readline';
import { program } from 'commander';
import { BankActivityEtl } from './etl.js';

program
    .version('1.0.0')
    .option('-i, --input <file>', 'Input CSV file')
    .option('-o, --output <file>', 'Output CSV file')
    .option('-a, --append', 'Append if the file already exists')
    .option('-w, --overwrite', 'Overwrite if the file already exists')
    .parse(process.argv);

const { input, output, append, overwrite } = program.opts();

const inputFileName = input || process.argv[2];
const outputFileName = output || inputFileName.replace('.csv', '_copy.csv');
const bankActivityEtl = new BankActivityEtl(inputFileName, outputFileName);

if (append) {
    bankActivityEtl.process(true);
} else if (overwrite) {
    bankActivityEtl.process(false);
} else if (fs.existsSync(outputFileName)) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

    rl.question(`This output file already exists, do you want to overwrite (o) it, append (a) to it, or cancel (c)? `, result => {
        if (result === 'a') {
            bankActivityEtl.process(true);
        } else if (result === 'o') {
            bankActivityEtl.process(false);
        }

        rl.close();
    });
} else {
    bankActivityEtl.process(false);
}

