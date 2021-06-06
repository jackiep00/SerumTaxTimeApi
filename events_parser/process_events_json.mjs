
import { Connection, PublicKey } from '@solana/web3.js';
import { Market, OpenOrders } from '@project-serum/serum';
import Database from 'better-sqlite3';

import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

require("dotenv").config(); // is this the right syntax for mjs?

var main = async function() {

    const db = new Database(dbPath);
    const waitTime = 50;

    var markets = JSON.parse(fs.readFileSync(marketsPath, 'utf8'));

    // Remove deprecated items
    markets = markets.filter((item, i, ar) => !item['deprecated']);
    

    for (var i = 0; i < markets.length; i++) {
        console.log(i);

        let marketMeta = markets[i];
        
        marketMeta['baseCurrency'] = marketMeta['name'].split('/')[0];
        marketMeta['quoteCurrency'] = marketMeta['name'].split('/')[1];
        
        let connection = new Connection(`${process.env.RPC}`); 
        let marketAddress = new PublicKey(marketMeta['address']);
        let programID = new PublicKey(marketMeta['programId']);

        // Contrary to the docs - you need to pass programID as well it seems
        let market = await Market.load(connection, marketAddress, {}, programID);

        marketMeta['_baseSplTokenDecimals'] = market._baseSplTokenDecimals
        marketMeta['_quoteSplTokenDecimals'] = market._quoteSplTokenDecimals

        console.log(marketMeta['name']);

        let loadTimestamp = new Date().toISOString();
        let events = await market.loadEventQueue(connection, 1000000);

        let marketEventsLength = events.length; 
        console.log(marketEventsLength);

        log('Pulling event queue at ' + loadTimestamp, INFO_LEVEL, marketMeta);

        let queueOffset = getQueueOffset(events, marketMeta, db);

        let newEvents = events.slice(0, queueOffset);

        await addOwnerMappings(newEvents, db, connection, marketMeta);
        insertCurrencyMeta(marketMeta, db);

        // Only insert filled events to save space
        let filledEvents = newEvents.filter((item, i, ar) => item.eventFlags['fill']);
        insertEvents(filledEvents, marketMeta, loadTimestamp, db);
        
        // Insert all events for more convenient matching
        insertStringEvents(newEvents, marketEventsLength, marketMeta, loadTimestamp, db);

        await new Promise(resolve => setTimeout(resolve, waitTime));

        
    }

}


await main();