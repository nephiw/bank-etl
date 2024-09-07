#!/usr/bin/env node
import { program } from 'commander';
import { BankActivityEtl } from './etl.js';

program
    .version('1.0.0')
    .option('-i, --input <file>', 'Input CSV file')
    .option('-o, --output <file>', 'Output CSV file')
    .parse(process.argv);

const { input, output } = program.opts();

const inputFileName = input || process.argv[2];
const outputFileName = output || inputFileName.replace('.csv', '_copy.csv');

const bankActivityEtl = new BankActivityEtl(inputFileName, outputFileName);
bankActivityEtl.process();
