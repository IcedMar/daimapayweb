require('dotenv').config(); // Load environment variables from .env file
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');
const { FieldValue } = require('firebase-admin/firestore');
const rateLimit = require('express-rate-limit');
const winston = require('winston'); // For logging
const cors = require('cors'); // Added CORS

// Initialize Firebase Admin SDk
const serviceAccount = JSON.parse(
    Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_JSON, 'base64').toString('utf-8')
);

// --- Global Error Handlers (VERY IMPORTANT FOR PRODUCTION) ---
process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION! Shutting down...', err.name, err.message, err.stack);
    logger.error('UNCAUGHT EXCEPTION! Shutting down...', { error: err.message, stack: err.stack, name: err.name });
    // Give a short grace period for logs to flush before exiting
    setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('UNHANDLED REJECTION! Shutting down...', reason);
    logger.error('UNHANDLED REJECTION! Shutting down...', { reason: reason, promise: promise });
    // Give a short grace period for logs to flush before exiting
    setTimeout(() => process.exit(1), 1000);
});

// Check if a Firebase app has already been initialized to avoid re-initialization errors
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount) // Use admin.credential.cert() with the parsed serviceAccount
    });
}

const firestore = admin.firestore();

// Firestore Collection References
const transactionsCollection = firestore.collection('transactions');
const salesCollection = firestore.collection('sales'); // This will store initial requests AND fulfillment details
const errorsCollection = firestore.collection('errors');
const safaricomFloatDocRef = firestore.collection('Saf_float').doc('current');
const africasTalkingFloatDocRef = firestore.collection('AT_Float').doc('current');
const failedReconciliationsCollection = firestore.collection('failed_reconciliations');
const reconciledTransactionsCollection = firestore.collection('reconciled_transactions');
const bonusHistoryCollection = firestore.collection('bonus_history');
const reversalTimeoutsCollection = firestore.collection('reversal_timeouts');
const safaricomDealerConfigRef = firestore.collection('mpesa_settings').doc('main_config');
const stkTransactionsCollection = firestore.collection('stk_transactions');

// M-Pesa API Credentials from .env
const CONSUMER_KEY = process.env.CONSUMER_KEY;
const CONSUMER_SECRET = process.env.CONSUMER_SECRET;
const SHORTCODE = process.env.BUSINESS_SHORT_CODE; // Your Paybill/Till number
const PASSKEY = process.env.PASSKEY;
const STK_CALLBACK_URL = process.env.CALLBACK_URL; // Your public URL for /stk-callback
const ANALYTICS_SERVER_URL = process.env.ANALYTICS_SERVER_URL; // Your analytics server URL

// Logger setup
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console(),
        // new winston.transports.File({ filename: 'error.log', level: 'error' }),
        // new winston.transports.File({ filename: 'combined.log' })
    ],
});

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// --- CORS Configuration ---
// Allow specific origins (recommended for production)
const allowedOrigins = [
     'https://daima-pay-portal.onrender.com',
    'https://daimapay-51406.web.app',
  'https://dpanalyticsserver.onrender.com'
];
app.use(cors({
    origin: function (origin, callback) {
        // allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Allowed HTTP methods
    credentials: true, // Allow cookies to be sent with requests (if needed)
    optionsSuccessStatus: 204 // Some legacy browsers (IE11, various SmartTVs) choke on 200
}));


// --- HELPER FUNCTIONS (PLACEHOLDERS - Implement these based on your existing code) ---

// Function to get Daraja access token
async function getAccessToken() {
    const auth = Buffer.from(`${CONSUMER_KEY}:${CONSUMER_SECRET}`).toString('base64');
    try {
        const response = await axios.get('https://api.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials', {
            headers: {
                'Authorization': `Basic ${auth}`
            }
        });
        return response.data.access_token;
    } catch (error) {
        logger.error('Error getting access token:', error.message);
        throw new Error('Failed to get M-Pesa access token.');
    }
}

// Function to generate the timestamp for M-Pesa API
function generateTimestamp() {
    const date = new Date();
    const year = date.getFullYear().toString();
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const day = date.getDate().toString().padStart(2, '0');
    const hour = date.getHours().toString().padStart(2, '0');
    const minute = date.getMinutes().toString().padStart(2, '0');
    const second = date.getSeconds().toString().padStart(2, '0');
    return `${year}${month}${day}${hour}${minute}${second}`;
}

// Function to generate password for STK Push
function generatePassword(shortcode, passkey, timestamp) {
    const str = shortcode + passkey + timestamp;
    return Buffer.from(str).toString('base64');
}

let cachedAirtimeToken = null;
let tokenExpiryTimestamp = 0;

// ✅ Safaricom dealer token
async function getCachedAirtimeToken() {
    const now = Date.now();
    if (cachedAirtimeToken && now < tokenExpiryTimestamp) {
        logger.info('🔑 Using cached dealer token');
        return cachedAirtimeToken;
    }
    try {
        const auth = Buffer.from(`${process.env.MPESA_AIRTIME_KEY}:${process.env.MPESA_AIRTIME_SECRET}`).toString('base64');
        const response = await axios.post(
            process.env.MPESA_GRANT_URL,
            {},
            {
                headers: {
                    Authorization: `Basic ${auth}`,
                    'Content-Type': 'application/json',
                },
            }
        );
        const token = response.data.access_token;
        cachedAirtimeToken = token;
        tokenExpiryTimestamp = now + 3599 * 1000;
        logger.info('✅ Fetched new dealer token.');
        return token;
    } catch (error) {
        logger.error('❌ Failed to get Safaricom airtime token:', {
            message: error.message,
            response_data: error.response ? error.response.data : 'N/A',
            stack: error.stack
        });
        throw new Error('Failed to obtain Safaricom airtime token.');
    }
}

// NEW: Cache variables for Dealer Service PIN
let cachedDealerServicePin = null;
let dealerPinExpiryTimestamp = 0;
const DEALER_PIN_CACHE_TTL = 10 * 60 * 1000; 

//service pin
async function generateServicePin(rawPin) {
    logger.debug('[generateServicePin] rawPin length:', rawPin ? rawPin.length : 'null');
    try {
        const encodedPin = Buffer.from(rawPin).toString('base64'); // Correct for Node.js
        logger.debug('[generateServicePin] encodedPin length:', encodedPin.length);
        return encodedPin;
    } catch (error) {
        logger.error('[generateServicePin] error:', error);
        throw new Error(`Service PIN generation failed: ${error.message}`);
    }
}

// NEW: Function to get dealer service PIN from Firestore with caching
async function getDealerServicePin() {
    const now = Date.now();
    if (cachedDealerServicePin && now < dealerPinExpiryTimestamp) {
        logger.info('🔑 Using cached dealer service PIN from memory.');
        return cachedDealerServicePin;
    }

    logger.info('🔄 Fetching dealer service PIN from Firestore (mpesa_settings/main_config/servicePin)...');
    try {
        const doc = await safaricomDealerConfigRef.get(); // This now points to mpesa_settings/main_config

        if (!doc.exists) {
            const errorMsg = 'Dealer service PIN configuration document (mpesa_settings/main_config) not found in Firestore. Please create it with a "servicePin" field.';
            logger.error(`❌ ${errorMsg}`);
            throw new Error(errorMsg);
        }

        const pin = doc.data().servicePin; // THIS IS THE KEY CHANGE for the field name

        if (!pin) {
            const errorMsg = 'Dealer service PIN field ("servicePin") not found in Firestore document (mpesa_settings/main_config). Please add it.';
            logger.error(`❌ ${errorMsg}`);
            throw new Error(errorMsg);
        }

        // Cache the retrieved PIN and set expiry
        cachedDealerServicePin = pin;
        dealerPinExpiryTimestamp = now + DEALER_PIN_CACHE_TTL;
        logger.info('✅ Successfully fetched and cached dealer service PIN from Firestore.');
        return pin;

    } catch (error) {
        logger.error('❌ Failed to retrieve dealer service PIN from Firestore:', {
            message: error.message,
            stack: error.stack
        });
        throw new Error(`Failed to retrieve dealer service PIN: ${error.message}`);
    }
}


// Placeholder for detecting carrier (You already have this in your C2B handler)
function detectCarrier(phoneNumber) {
    const normalized = phoneNumber.replace(/^(\+254|254)/, '0').trim();
    if (normalized.length !== 10 || !normalized.startsWith('0')) {
        logger.debug(`Invalid phone number format for carrier detection: ${phoneNumber}`);
        return 'Unknown';
    }
    const prefix3 = normalized.substring(1, 4);

    const safaricom = new Set([
        '110', '111', '112', '113', '114', '115', '116', '117', '118', '119',
        '700', '701', '702', '703', '704', '705', '706', '707', '708', '709',
        '710', '711', '712', '713', '714', '715', '716', '717', '718', '719',
        '720', '721', '722', '723', '724', '725', '726', '727', '728', '729',
        '740', '741', '742', '743', '744', '745', '746', '748', '749',
        '757', '758', '759',
        '768', '769',
        '790', '791', '792', '793', '794', '795', '796', '797', '798', '799'
    ]);
    const airtel = new Set([
        '100', '101', '102', '103', '104', '105', '106', '107', '108', '109',
        '730', '731', '732', '733', '734', '735', '736', '737', '738', '739',
        '750', '751', '752', '753', '754', '755', '756',
        '780', '781', '782', '783', '784', '785', '786', '787', '788', '789'
    ]);
    const telkom = new Set([
        '770', '771', '772', '773', '774', '775', '776', '777', '778', '779'
    ]);
    const equitel = new Set([
        '764', '765', '766', '767',
    ]);
    const faiba = new Set([
        '747',
    ]);

    if (safaricom.has(prefix3)) return 'Safaricom';
    if (airtel.has(prefix3)) return 'Airtel';
    if (telkom.has(prefix3)) return 'Telkom';
    if (equitel.has(prefix3)) return 'Equitel';
    if (faiba.has(prefix3)) return 'Faiba';
    return 'Unknown';
}

function normalizeReceiverPhoneNumber(num) {
    let normalized = String(num).replace(/^(\+254|254)/, '0').trim();
    if (normalized.startsWith('0') && normalized.length === 10) {
        return normalized.slice(1); // Converts '0712345678' to '712345678'
    }
    if (normalized.length === 9 && !normalized.startsWith('0')) {
        return normalized;
    }
    logger.warn(`Phone number could not be normalized to 7XXXXXXXX format for Safaricom: ${num}. Returning as is.`);
    return num; // Return as is, let the API potentially fail for incorrect format
}

// ✅ Send Safaricom dealer airtime
async function sendSafaricomAirtime(receiverNumber, amount) {
    try {
        const token = await getCachedAirtimeToken();
        const normalizedReceiver = normalizeReceiverPhoneNumber(receiverNumber);
        const adjustedAmount = Math.round(amount * 100); // Amount in cents

        if (!process.env.DEALER_SENDER_MSISDN || !process.env.MPESA_AIRTIME_URL) {
            const missingEnvError = 'Missing Safaricom Dealer API environment variables (DEALER_SENDER_MSISDN, MPESA_AIRTIME_URL). DEALER_SERVICE_PIN is now fetched from Firestore.';
            logger.error(missingEnvError);
            return { status: 'FAILED', message: missingEnvError };
        }

        const rawDealerPin = await getDealerServicePin(); 
        const servicePin = await generateServicePin(rawDealerPin); 

        const body = {
            senderMsisdn: process.env.DEALER_SENDER_MSISDN,
            amount: adjustedAmount,
            servicePin: servicePin,
            receiverMsisdn: normalizedReceiver,
        };

        const response = await axios.post(
            process.env.MPESA_AIRTIME_URL,
            body,
            {
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            }
        );

        let safaricomInternalTransId = null;
        let newSafaricomFloatBalance = null;

        // --- CORRECTED: Check Safaricom API response status for actual success ---
        const isSuccess = response.data && response.data.responseStatus === '200';

        if (response.data && response.data.responseDesc) {
            const desc = response.data.responseDesc;
            const idMatch = desc.match(/^(R\d{6}\.\d{4}\.\d{6})/); // Regex for the transaction ID
            if (idMatch && idMatch[1]) {
                safaricomInternalTransId = idMatch[1];
            }
            const balanceMatch = desc.match(/New balance is Ksh\. (\d+(?:\.\d{2})?)/); // Regex for the balance
            if (balanceMatch && balanceMatch[1]) {
                newSafaricomFloatBalance = parseFloat(balanceMatch[1]);
            }else {
                logger.warn(`⚠️ Could not extract new Safaricom float balance from response description: "${desc}"`);
            }
        }
        // Always log the full response from Safaricom for debugging purposes
        logger.info('✅ Safaricom dealer airtime API response:', { receiver: normalizedReceiver, amount: amount, response_data: response.data });

        if (isSuccess) {
            return {
                status: 'SUCCESS',
                message: 'Safaricom airtime sent',
                data: response.data,
                safaricomInternalTransId: safaricomInternalTransId,
                newSafaricomFloatBalance: newSafaricomFloatBalance,
            };
        } else {
            // If the status code indicates failure, return FAILED
            const errorMessage = `Safaricom Dealer API reported failure (Status: ${response.data.responseStatus || 'N/A'}): ${response.data.responseDesc || 'Unknown reason'}`;
            logger.warn(`⚠️ Safaricom dealer airtime send reported non-success:`, {
                receiver: receiverNumber,
                amount: amount,
                response_data: response.data,
                errorMessage: errorMessage
            });
            return {
                status: 'FAILED',
                message: errorMessage,
                error: response.data, // Provide the full response for debugging
            };
        }
    } catch (error) {
        logger.error('❌ Safaricom dealer airtime send failed (exception caught):', {
            receiver: receiverNumber,
            amount: amount,
            message: error.message,
            response_data: error.response ? error.response.data : 'N/A',
            stack: error.stack
        });
        return {
            status: 'FAILED',
            message: 'Safaricom airtime send failed due to network/API error',
            error: error.response ? error.response.data : error.message,
        };
    }
}

// --- Africa's Talking Initialization ---
const AfricasTalking = require('africastalking');
const africastalking = AfricasTalking({
    apiKey: process.env.AT_API_KEY,
    username: process.env.AT_USERNAME
});

// Function to send Africa's Talking Airtime
async function sendAfricasTalkingAirtime(phoneNumber, amount, carrier) {
    let normalizedPhone = phoneNumber;

    // AT expects E.164 format (+254XXXXXXXXX)
    if (phoneNumber.startsWith('0')) {
        normalizedPhone = '+254' + phoneNumber.slice(1);
    } else if (phoneNumber.startsWith('254') && !phoneNumber.startsWith('+')) {
        normalizedPhone = '+' + phoneNumber;
    } else if (!phoneNumber.startsWith('+254')) {
        logger.error('[sendAfricasTalkingAirtime] Invalid phone format:', { phoneNumber: phoneNumber });
        return {
            status: 'FAILED',
            message: 'Invalid phone number format for Africa\'s Talking',
            details: {
                error: 'Phone must start with +254, 254, or 0'
            }
        };
    }

    if (!process.env.AT_API_KEY || !process.env.AT_USERNAME) {
        logger.error('Missing Africa\'s Talking API environment variables.');
        return { status: 'FAILED', message: 'Missing Africa\'s Talking credentials.' };
    }

    try {
        const result = await africastalking.AIRTIME.send({
            recipients: [{
                phoneNumber: normalizedPhone,
                amount: amount,
                currencyCode: 'KES'
            }]
        });

        // Defensive check
        const response = result?.responses?.[0];
        const status = response?.status;
        const errorMessage = response?.errorMessage;

        if (status === 'Sent' && errorMessage === 'None') {
            logger.info(`✅ Africa's Talking airtime successfully sent to ${carrier}:`, {
                recipient: normalizedPhone,
                amount: amount,
                at_response: result
            });
            return {
                status: 'SUCCESS',
                message: 'Africa\'s Talking airtime sent',
                data: result,
            };
        } else {
            logger.error(`❌ Africa's Talking airtime send indicates non-success for ${carrier}:`, {
                recipient: normalizedPhone,
                amount: amount,
                at_response: result
            });
            return {
                status: 'FAILED',
                message: 'Africa\'s Talking airtime send failed or not successful.',
                error: result,
            };
        }

    } catch (error) {
        logger.error(`❌ Africa's Talking airtime send failed for ${carrier} (exception caught):`, {
            recipient: normalizedPhone,
            amount: amount,
            message: error.message,
            stack: error.stack
        });
        return {
            status: 'FAILED',
            message: 'Africa\'s Talking airtime send failed (exception)',
            error: error.message,
        };
    }
}

// Helper function to notify the offline server (add this somewhere in your server.js)
async function notifyOfflineServerForFulfillment(transactionDetails) {
    try {
        const offlineServerUrl = process.env.OFFLINE_SERVER_FULFILLMENT_URL;
        if (!offlineServerUrl) {
            logger.error('OFFLINE_SERVER_FULFILLMENT_URL is not set in environment variables. Cannot notify offline server.');
            return { success: false, message: 'Offline server URL not configured.' };
        }

        // Send a POST request to your offline server
        const response = await axios.post(offlineServerUrl, transactionDetails);

        logger.info(`✅ Notified offline server for fulfillment of ${transactionDetails.checkoutRequestID}. Offline server response:`, response.data);
        return { success: true, responseData: response.data };

    } catch (error) {
        logger.error(`❌ Failed to notify offline server for fulfillment of ${transactionDetails.checkoutRequestID}:`, {
            message: error.message,
            statusCode: error.response ? error.response.status : 'N/A',
            responseData: error.response ? error.response.data : 'N/A',
            stack: error.stack
        });

        // Log this critical error to Firestore's errorsCollection
        await errorsCollection.add({
            type: 'OFFLINE_SERVER_NOTIFICATION_FAILED',
            checkoutRequestID: transactionDetails.checkoutRequestID,
            error: error.message,
            offlineServerResponse: error.response ? error.response.data : null,
            payloadSent: transactionDetails,
            createdAt: FieldValue.serverTimestamp(),
        });

        return { success: false, message: 'Failed to notify offline server.' };
    }
}


function generateSecurityCredential(password) {
    const certificatePath = '/etc/secrets/ProductionCertificate.cer';

    try {
        console.log('🔹 Reading the public key certificate...');
        const publicKey = fs.readFileSync(certificatePath, 'utf8');

        console.log('✅ Certificate loaded successfully.');
        console.log('🔹 Encrypting the password...');
        const encryptedBuffer = crypto.publicEncrypt(
            {
                key: publicKey,
                padding: crypto.constants.RSA_PKCS1_PADDING,
            },
            Buffer.from(password, 'utf8')
        );

        return encryptedBuffer.toString('base64');
    } catch (error) {
        console.error('❌ Error generating security credential:', error.message);
        return null;
    }
}

// --- NEW: Daraja Reversal Function ---
async function initiateDarajaReversal(transactionId, amount, receiverMsisdn) { 
    logger.info(`🔄 Attempting Daraja reversal for TransID: ${transactionId}, Amount: ${amount}`);
    try {
        const accessToken = await getDarajaAccessToken(); // Function to get Daraja access token

        if (!accessToken) {
            throw new Error("Failed to get Daraja access token for reversal.");
        }

        const url = process.env.MPESA_REVERSAL_URL; 
        const shortCode = process.env.MPESA_SHORTCODE; 
        const initiator = process.env.MPESA_INITIATOR_NAME; 
        const password=process.env.MPESA_SECURITY_PASSWORD;
        const securityCredential = generateSecurityCredential(password);  
        

        if (!url || !shortCode || !initiator || !securityCredential) {
            throw new Error("Missing Daraja reversal environment variables.");
        }

        const payload = {
            Initiator: initiator,
            SecurityCredential: securityCredential, // Use your actual security credential
            CommandID: "TransactionReversal",
            TransactionID: transactionId, // The M-Pesa TransID to be reversed
            Amount: amount, // The amount to reverse
            ReceiverParty: shortCode, // Your Short Code
            RecieverIdentifierType: "11",
            QueueTimeOutURL: process.env.MPESA_REVERSAL_QUEUE_TIMEOUT_URL, // URL for timeout callbacks
            ResultURL: process.env.MPESA_REVERSAL_RESULT_URL, // URL for result callbacks
            Remarks: `Airtime dispatch failed for ${transactionId}`,
            Occasion: "Failed Airtime Topup"
        };

        const headers = {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${accessToken}`
        };

        const response = await axios.post(url, payload, { headers });

        logger.info(`✅ Daraja Reversal API response for TransID ${transactionId}:`, response.data);

        if (response.data && response.data.ResponseCode === '0') {
            return {
                success: true,
                message: "Reversal request accepted by Daraja.",
                data: response.data,
                // You might store the ConversationID for tracking if provided
                conversationId: response.data.ConversationID || null,
            };
        } else {
            const errorMessage = response.data ?
                `Daraja reversal request failed: ${response.data.ResponseDescription || 'Unknown error'}` :
                'Daraja reversal request failed with no response data.';
            logger.error(`❌ Daraja reversal request not accepted for TransID ${transactionId}: ${errorMessage}`);
            return {
                success: false,
                message: errorMessage,
                data: response.data,
            };
        }

    } catch (error) {
        const errorData = error.response ? error.response.data : error.message;
        logger.error(`❌ Exception during Daraja reversal for TransID ${transactionId}:`, {
            error: errorData,
            stack: error.stack
        });
        return {
            success: false,
            message: `Exception in reversal process: ${errorData.errorMessage || error.message}`,
            error: errorData
        };
    }
}

async function updateCarrierFloatBalance(carrierLogicalName, amount) {
    return firestore.runTransaction(async t => {
        let floatDocRef;
        if (carrierLogicalName === 'safaricomFloat') {
            floatDocRef = safaricomFloatDocRef;
        } else if (carrierLogicalName === 'africasTalkingFloat') {
            floatDocRef = africasTalkingFloatDocRef;
        } else {
            const errorMessage = `Invalid float logical name provided: ${carrierLogicalName}`;
            logger.error(`❌ ${errorMessage}`);
            throw new Error(errorMessage);
        }

        const floatDocSnapshot = await t.get(floatDocRef);

        let currentFloat = 0;
        if (floatDocSnapshot.exists) {
            currentFloat = parseFloat(floatDocSnapshot.data().balance); // Assuming 'balance' field as per your frontend
            if (isNaN(currentFloat)) {
                const errorMessage = `Float balance in document '${carrierLogicalName}' is invalid!`;
                logger.error(`❌ ${errorMessage}`);
                throw new Error(errorMessage);
            }
        } else {
            // If the document doesn't exist, create it with initial balance 0
            logger.warn(`Float document '${carrierLogicalName}' not found. Initializing with balance 0.`);
            t.set(floatDocRef, { balance: 0, lastUpdated: FieldValue.serverTimestamp() }); // Use FieldValue.serverTimestamp()
            currentFloat = 0; // Set currentFloat to 0 for this transaction's calculation
        }

        const newFloat = currentFloat + amount; // amount can be negative for debit
        if (amount < 0 && newFloat < 0) {
            const errorMessage = `Attempt to debit ${carrierLogicalName} float below zero. Current: ${currentFloat}, Attempted debit: ${-amount}`;
            logger.warn(`⚠️ ${errorMessage}`);
            throw new Error('Insufficient carrier-specific float balance for this transaction.');
        }

        t.update(floatDocRef, { balance: newFloat, lastUpdated: FieldValue.serverTimestamp() }); // Use FieldValue.serverTimestamp()
        logger.info(`✅ Updated ${carrierLogicalName} float balance. Old: ${currentFloat}, New: ${newFloat}, Change: ${amount}`);
        return { success: true, newBalance: newFloat };
    });
}

/**
 * Processes the airtime fulfillment for a given transaction.
 * This function is designed to be called by both C2B confirmation and STK Push callback.
 *
 * @param {object} params - The parameters for fulfillment.
 * @param {string} params.transactionId - The unique M-Pesa transaction ID (TransID or CheckoutRequestID).
 * @param {number} params.originalAmountPaid - The original amount paid by the customer.
 * @param {string} params.payerMsisdn - The phone number of the customer who paid.
 * @param {string} params.payerName - The name of the customer (optional, can be null for STK Push).
 * @param {string} params.topupNumber - The recipient phone number for airtime.
 * @param {string} params.sourceCallbackData - The raw callback data from M-Pesa (C2B or STK Push).
 * @param {string} params.requestType - 'C2B' or 'STK_PUSH' to differentiate logging/storage.
 * @param {string|null} [params.relatedSaleId=null] - Optional: saleId if already created (e.g., from STK Push initial request).
 * @returns {Promise<object>} - An object indicating success/failure and final status.
 */

async function processAirtimeFulfillment({
    transactionId,
    originalAmountPaid,
    payerMsisdn,
    payerName,
    topupNumber,
    sourceCallbackData,
    requestType,
    relatedSaleId = null
}) {
    const now = FieldValue.serverTimestamp(); // Use server timestamp for consistency
    logger.info(`Starting airtime fulfillment for ${requestType} transaction: ${transactionId}`);

    let airtimeDispatchStatus = 'FAILED';
    let airtimeDispatchResult = null;
    let saleErrorMessage = null;
    let airtimeProviderUsed = null;
    let finalSaleId = relatedSaleId; // Use existing saleId if provided

    try {
        // --- Input Validation (amount range - moved from C2B, now applies to both) ---
        // Note: For STK Push, amount validation happens before dispatch.
        // For C2B, it's here because the initial recording happens before this logic.
        const MIN_AMOUNT = 5;
        const MAX_AMOUNT = 5000;
        const amountInt = Math.round(parseFloat(originalAmountPaid));

        if (amountInt < MIN_AMOUNT || amountInt > MAX_AMOUNT) {
            const errorMessage = `Transaction amount ${amountInt} is outside allowed range (${MIN_AMOUNT} - ${MAX_AMOUNT}).`;
            logger.warn(`🛑 ${errorMessage} Initiating reversal for ${transactionId}.`);
            await errorsCollection.add({
                type: 'AIRTIME_FULFILLMENT_ERROR',
                subType: 'INVALID_AMOUNT_RANGE',
                error: errorMessage,
                transactionId: transactionId,
                originalAmount: originalAmountPaid,
                payerMsisdn: payerMsisdn,
                topupNumber: topupNumber,
                requestType: requestType,
                createdAt: now,
            });

            // Update transaction status before attempting reversal
            await transactionsCollection.doc(transactionId).update({
                status: 'RECEIVED_FULFILLMENT_FAILED',
                fulfillmentStatus: 'FAILED_INVALID_AMOUNT',
                errorMessage: errorMessage,
                lastUpdated: now,
            });

            const reversalResult = await initiateDarajaReversal(transactionId, originalAmountPaid, payerMsisdn);
            if (reversalResult.success) {
                logger.info(`✅ Reversal initiated for invalid amount ${amountInt} on transaction ${transactionId}`);
                await reconciledTransactionsCollection.doc(transactionId).set({
                    transactionId: transactionId,
                    amount: originalAmountPaid,
                    mpesaNumber: payerMsisdn,
                    reversalInitiatedAt: now,
                    reversalRequestDetails: reversalResult.data,
                    originalCallbackData: sourceCallbackData,
                    status: 'REVERSAL_INITIATED',
                    createdAt: now,
                }, { merge: true });
                await transactionsCollection.doc(transactionId).update({
                    status: 'REVERSAL_PENDING_CONFIRMATION',
                    lastUpdated: now,
                    reversalDetails: reversalResult.data,
                    errorMessage: reversalResult.message,
                    reversalAttempted: true,
                });
                return { success: true, status: 'REVERSAL_INITIATED_INVALID_AMOUNT' }; // Return success as reversal was initiated
            } else {
                logger.error(`❌ Reversal failed for invalid amount ${amountInt} for ${transactionId}: ${reversalResult.message}`);
                await failedReconciliationsCollection.doc(transactionId).set({
                    transactionId: transactionId,
                    amount: originalAmountPaid,
                    mpesaNumber: payerMsisdn,
                    reversalAttemptedAt: now,
                    reversalFailureDetails: reversalResult.error,
                    originalCallbackData: sourceCallbackData,
                    reason: `Reversal initiation failed for invalid amount: ${reversalResult.message}`,
                    createdAt: now,
                }, { merge: true });
                await transactionsCollection.doc(transactionId).update({
                    status: 'REVERSAL_INITIATION_FAILED',
                    lastUpdated: now,
                    reversalDetails: reversalResult.error,
                    errorMessage: `Reversal initiation failed for invalid amount: ${reversalResult.message}`,
                    reversalAttempted: true,
                });
                return { success: false, status: 'REVERSAL_FAILED_INVALID_AMOUNT', error: reversalResult.message };
            }
        }


        // --- Determine target carrier ---
        const targetCarrier = detectCarrier(topupNumber);
        if (targetCarrier === 'Unknown') {
            const errorMessage = `Unsupported carrier prefix for airtime top-up: ${topupNumber}`;
            logger.error(`❌ ${errorMessage}`, { TransID: transactionId, topupNumber: topupNumber });
            await errorsCollection.add({
                type: 'AIRTIME_FULFILLMENT_ERROR',
                subType: 'UNKNOWN_CARRIER',
                error: errorMessage,
                transactionId: transactionId,
                requestType: requestType,
                createdAt: now,
            });
            await transactionsCollection.doc(transactionId).update({
                status: 'RECEIVED_FULFILLMENT_FAILED',
                fulfillmentStatus: 'FAILED_UNKNOWN_CARRIER',
                errorMessage: errorMessage,
                lastUpdated: now,
            });
            return { success: false, status: 'FAILED_UNKNOWN_CARRIER', error: errorMessage };
        }

        // --- FETCH BONUS SETTINGS AND CALCULATE FINAL AMOUNT TO DISPATCH ---
        const bonusDocRef = firestore.collection('airtime_bonuses').doc('current_settings');
        const bonusDocSnap = await bonusDocRef.get();

        let safaricomBonus = 0;
        let atBonus = 0;

        if (bonusDocSnap.exists) {
            safaricomBonus = bonusDocSnap.data()?.safaricomPercentage ?? 0;
            atBonus = bonusDocSnap.data()?.africastalkingPercentage ?? 0;
        } else {
            logger.warn('Bonus settings document does not exist. Skipping bonus application.');
        }

        let finalAmountToDispatch = originalAmountPaid;
        let bonusApplied = 0;

        // Custom rounding: 0.1–0.4 => 0, 0.5–0.9 => 1
        const customRound = (value) => {
            const decimalPart = value % 1;
            const integerPart = Math.floor(value);
            return decimalPart >= 0.5 ? integerPart + 1 : integerPart;
        };

        // Apply bonus with optional rounding
        const applyBonus = (amount, percentage, label, round = false) => {
            const rawBonus = amount * (percentage / 100);
            const bonus = round ? customRound(rawBonus) : rawBonus;
            const total = amount + bonus;
            logger.info(
                `Applying ${percentage}% ${label} bonus. Original: ${amount}, Bonus: ${bonus} (${round ? 'rounded' : 'raw'}), Final: ${total}`
            );
            return { total, bonus, rawBonus };
        };

        // Normalize carrier name to lowercase
        const carrierNormalized = targetCarrier.toLowerCase();

        if (carrierNormalized === 'safaricom' && safaricomBonus > 0) {
            const result = applyBonus(originalAmountPaid, safaricomBonus, 'Safaricom', false); // No rounding
            finalAmountToDispatch = result.total;
            bonusApplied = result.rawBonus;
        } else if (['airtel', 'telkom', 'equitel', 'faiba'].includes(carrierNormalized) && atBonus > 0) {
            const result = applyBonus(originalAmountPaid, atBonus, 'AfricasTalking', true); // Use custom rounding
            finalAmountToDispatch = result.total;
            bonusApplied = result.bonus;
        }

        logger.info(`Final amount to dispatch for ${transactionId}: ${finalAmountToDispatch}`);

        // --- Initialize or Update sale document ---
        const saleData = {
            relatedTransactionId: transactionId,
            topupNumber: topupNumber,
            originalAmountPaid: originalAmountPaid,
            amount: finalAmountToDispatch, // This is the amount actually dispatched (original + bonus)
            bonusApplied: bonusApplied, // Store the bonus amount
            carrier: targetCarrier, // Use the detected carrier
            status: 'PENDING_DISPATCH',
            dispatchAttemptedAt: now,
            lastUpdated: now,
            requestType: requestType, // C2B or STK_PUSH
            // createdAt will be set if this is a new document, or remain if it's an update
        };

        if (finalSaleId) {
            // If relatedSaleId exists (from STK Push initial request), update it
            const saleDoc = await salesCollection.doc(finalSaleId).get();
            if (saleDoc.exists) {
                await salesCollection.doc(finalSaleId).update(saleData);
                logger.info(`✅ Updated existing sale document ${finalSaleId} for TransID ${transactionId} with fulfillment details.`);
            } else {
                // If ID was provided but document doesn't exist (e.g., deleted), create new one
                const newSaleRef = salesCollection.doc();
                finalSaleId = newSaleRef.id;
                await newSaleRef.set({ saleId: finalSaleId, createdAt: now, ...saleData });
                logger.warn(`⚠️ Sale document ${relatedSaleId} not found. Created new sale document ${finalSaleId} for TransID ${transactionId}.`);
            }
        } else {
            // Create a new sale document (typical for C2B)
            const newSaleRef = salesCollection.doc();
            finalSaleId = newSaleRef.id;
            await newSaleRef.set({ saleId: finalSaleId, createdAt: now, ...saleData });
            logger.info(`✅ Initialized new sale document ${finalSaleId} in 'sales' collection for TransID ${transactionId}.`);
        }

        // --- Conditional Airtime Dispatch Logic based on Carrier ---
        if (targetCarrier === 'Safaricom') {
            try {
                await updateCarrierFloatBalance('safaricomFloat', -finalAmountToDispatch);
                airtimeProviderUsed = 'SafaricomDealer';
                airtimeDispatchResult = await sendSafaricomAirtime(topupNumber, finalAmountToDispatch);

                if (airtimeDispatchResult && airtimeDispatchResult.status === 'SUCCESS') {
                    airtimeDispatchStatus = 'COMPLETED';
                    logger.info(`✅ Safaricom airtime successfully sent via Dealer Portal for sale ${finalSaleId}.`);
                } else {
                    saleErrorMessage = airtimeDispatchResult?.error || 'Safaricom Dealer Portal failed with unknown error.';
                    logger.warn(`⚠️ Safaricom Dealer Portal failed for TransID ${transactionId}. Attempting fallback to Africastalking. Error: ${saleErrorMessage}`);

                    // Refund Safaricom float, as primary attempt failed
                    await updateCarrierFloatBalance('safaricomFloat', finalAmountToDispatch);
                    logger.info(`✅ Refunded Safaricom float for TransID ${transactionId}: +${finalAmountToDispatch}`);

                    // Attempt fallback via Africa's Talking (debit AT float)
                    await updateCarrierFloatBalance('africasTalkingFloat', -finalAmountToDispatch);
                    airtimeProviderUsed = 'AfricasTalkingFallback';
                    airtimeDispatchResult = await sendAfricasTalkingAirtime(topupNumber, finalAmountToDispatch, targetCarrier);

                    if (airtimeDispatchResult && airtimeDispatchResult.status === 'SUCCESS') {
                        airtimeDispatchStatus = 'COMPLETED';
                        logger.info(`✅ Safaricom fallback airtime successfully sent via AfricasTalking for sale ${finalSaleId}.`);
                        // NEW: Adjust Africa's Talking float for 4% commission
                        const commissionAmount = parseFloat((originalAmountPaid * 0.04).toFixed(2));
                        await updateCarrierFloatBalance('africasTalkingFloat', commissionAmount);
                        logger.info(`✅ Credited Africa's Talking float with ${commissionAmount} (4% commission) for TransID ${transactionId}.`);
                    } else {
                        saleErrorMessage = airtimeDispatchResult ? airtimeDispatchResult.error : 'AfricasTalking fallback failed with no specific error.';
                        logger.error(`❌ Safaricom fallback via AfricasTalking failed for sale ${finalSaleId}: ${saleErrorMessage}`);
                    }
                }
            } catch (dispatchError) {
                saleErrorMessage = `Safaricom primary dispatch process failed (or float debit failed): ${dispatchError.message}`;
                logger.error(`❌ Safaricom primary dispatch process failed for TransID ${transactionId}: ${dispatchError.message}`);
            }

        } else if (['Airtel', 'Telkom', 'Equitel', 'Faiba'].includes(targetCarrier)) {
            // Directly dispatch via Africa's Talking
            try {
                await updateCarrierFloatBalance('africasTalkingFloat', -finalAmountToDispatch);
                airtimeProviderUsed = 'AfricasTalkingDirect';
                airtimeDispatchResult = await sendAfricasTalkingAirtime(topupNumber, finalAmountToDispatch, targetCarrier);

                if (airtimeDispatchResult && airtimeDispatchResult.status === 'SUCCESS') {
                    airtimeDispatchStatus = 'COMPLETED';
                    logger.info(`✅ AfricasTalking airtime successfully sent directly for sale ${finalSaleId}.`);
                    // NEW: Adjust Africa's Talking float for 4% commission
                    const commissionAmount = parseFloat((originalAmountPaid * 0.04).toFixed(2));
                    await updateCarrierFloatBalance('africasTalkingFloat', commissionAmount);
                    logger.info(`✅ Credited Africa's Talking float with ${commissionAmount} (4% commission) for TransID ${transactionId}.`);
                } else {
                    saleErrorMessage = airtimeDispatchResult ? airtimeDispatchResult.Safaricom : 'AfricasTalking direct dispatch failed with no specific error.';
                    logger.error(`❌ AfricasTalking direct dispatch failed for sale ${finalSaleId}: ${saleErrorMessage}`);
                }
            } catch (dispatchError) {
                saleErrorMessage = `AfricasTalking direct dispatch process failed (or float debit failed): ${dispatchError.message}`;
                logger.error(`❌ AfricasTalking direct dispatch process failed for TransID ${transactionId}: ${dispatchError.message}`);
            }
        } else {
            // This case should ideally be caught by the initial detectCarrier check, but good for robustness
            saleErrorMessage = `No valid dispatch path for carrier: ${targetCarrier}`;
            logger.error(`❌ ${saleErrorMessage} for TransID ${transactionId}`);
            await errorsCollection.add({
                type: 'AIRTIME_FULFILLMENT_ERROR',
                subType: 'NO_DISPATCH_PATH',
                error: saleErrorMessage,
                transactionId: transactionId,
                requestType: requestType,
                createdAt: now,
            });
        }

        const updateSaleFields = {
            lastUpdated: now,
            dispatchResult: airtimeDispatchResult?.data || airtimeDispatchResult?.error || airtimeDispatchResult,
            airtimeProviderUsed: airtimeProviderUsed,
        };

        // If airtime dispatch was COMPLETELY successful
        if (airtimeDispatchStatus === 'COMPLETED') {
            updateSaleFields.status = airtimeDispatchStatus;

            // Only update Safaricom float balance from API response if Safaricom Dealer was used and successful
            if (targetCarrier === 'Safaricom' && airtimeDispatchResult && airtimeDispatchResult.newSafaricomFloatBalance !== undefined && airtimeProviderUsed === 'SafaricomDealer') {
                try {
                    await safaricomFloatDocRef.update({
                        balance: airtimeDispatchResult.newSafaricomFloatBalance,
                        lastUpdated: now
                    });
                    logger.info(`✅ Safaricom float balance directly updated from API response for TransID ${transactionId}. New balance: ${airtimeDispatchResult.newSafaricomFloatBalance}`);
                } catch (floatUpdateErr) {
                    logger.error(`❌ Failed to directly update Safaricom float from API response for TransID ${transactionId}:`, {
                        error: floatUpdateErr.message, reportedBalance: airtimeDispatchResult.newSafaricomFloatBalance
                    });
                    const reportedBalanceForError = airtimeDispatchResult.newSafaricomFloatBalance !== undefined ? airtimeDispatchResult.newSafaricomFloatBalance : 'N/A';
                    await errorsCollection.add({
                        type: 'FLOAT_RECONCILIATION_WARNING',
                        subType: 'SAFARICOM_REPORTED_BALANCE_UPDATE_FAILED',
                        error: `Failed to update Safaricom float with reported balance: ${floatUpdateErr.message}`,
                        transactionId: transactionId,
                        saleId: finalSaleId,
                        reportedBalance: reportedBalanceForError,
                        createdAt: now,
                    });
                }
            }
            await salesCollection.doc(finalSaleId).update(updateSaleFields);
            logger.info(`✅ Updated sale document ${finalSaleId} with dispatch result (COMPLETED).`);

            // Also update the main transaction status to fulfilled
            await transactionsCollection.doc(transactionId).update({
                status: 'COMPLETED_AND_FULFILLED',
                fulfillmentStatus: airtimeDispatchStatus,
                fulfillmentDetails: airtimeDispatchResult,
                lastUpdated: now,
                airtimeProviderUsed: airtimeProviderUsed,
            });
            logger.info(`✅ Transaction ${transactionId} marked as COMPLETED_AND_FULFILLED.`);
            return { success: true, status: 'COMPLETED_AND_FULFILLED' };

        } else {
            // Airtime dispatch ultimately failed (either primary or fallback)
            saleErrorMessage = saleErrorMessage || 'Airtime dispatch failed with no specific error message.';
            logger.error(`❌ Airtime dispatch ultimately failed for sale ${finalSaleId} (TransID ${transactionId}):`, {
                error_message: saleErrorMessage,
                carrier: targetCarrier,
                topupNumber: topupNumber,
                originalAmountPaid: originalAmountPaid,
                finalAmountDispatched: finalAmountToDispatch,
                airtimeResponse: airtimeDispatchResult,
                sourceCallbackData: sourceCallbackData,
            });
            await errorsCollection.add({
                type: 'AIRTIME_FULFILLMENT_ERROR',
                subType: 'AIRTIME_DISPATCH_FAILED',
                error: saleErrorMessage,
                transactionId: transactionId,
                saleId: finalSaleId,
                sourceCallbackData: sourceCallbackData,
                airtimeApiResponse: airtimeDispatchResult,
                providerAttempted: airtimeProviderUsed,
                requestType: requestType,
                createdAt: now,
            });

            updateSaleFields.status = 'FAILED_DISPATCH_API';
            updateSaleFields.errorMessage = saleErrorMessage;
            await salesCollection.doc(finalSaleId).update(updateSaleFields);
            logger.info(`✅ Updated sale document ${finalSaleId} with dispatch result (FAILED).`);

            // --- Initiate Reversal if airtime dispatch failed ---
            logger.warn(`🛑 Airtime dispatch ultimately failed for TransID ${transactionId}. Initiating Daraja reversal.`);

            // Update main transaction status to reflect immediate failure
            await transactionsCollection.doc(transactionId).update({
                status: 'RECEIVED_FULFILLMENT_FAILED',
                fulfillmentStatus: 'FAILED_DISPATCH_API',
                fulfillmentDetails: airtimeDispatchResult,
                errorMessage: saleErrorMessage,
                lastUpdated: now,
                airtimeProviderUsed: airtimeProviderUsed,
                reversalAttempted: true,
            });

            const reversalResult = await initiateDarajaReversal(transactionId, originalAmountPaid, payerMsisdn);

            if (reversalResult.success) {
                logger.info(`✅ Daraja reversal initiated successfully for TransID ${transactionId}.`);
                await reconciledTransactionsCollection.doc(transactionId).set({
                    transactionId: transactionId,
                    amount: originalAmountPaid,
                    mpesaNumber: payerMsisdn,
                    reversalInitiatedAt: now,
                    reversalRequestDetails: reversalResult.data,
                    originalCallbackData: sourceCallbackData,
                    status: 'REVERSAL_INITIATED',
                    createdAt: now,
                }, { merge: true });
                await transactionsCollection.doc(transactionId).update({
                    status: 'REVERSAL_PENDING_CONFIRMATION',
                    lastUpdated: now,
                    reversalDetails: reversalResult.data,
                    errorMessage: reversalResult.message,
                });
                return { success: true, status: 'REVERSAL_INITIATED' };
            } else {
                logger.error(`❌ Daraja reversal failed to initiate for TransID ${transactionId}: ${reversalResult.message}`);
                await failedReconciliationsCollection.doc(transactionId).set({
                    transactionId: transactionId,
                    amount: originalAmountPaid,
                    mpesaNumber: payerMsisdn,
                    reversalAttemptedAt: now,
                    reversalFailureDetails: reversalResult.error,
                    originalCallbackData: sourceCallbackData,
                    reason: reversalResult.message,
                    createdAt: now,
                }, { merge: true });
                await transactionsCollection.doc(transactionId).update({
                    status: 'REVERSAL_INITIATION_FAILED',
                    lastUpdated: now,
                    reversalDetails: reversalResult.error,
                    errorMessage: `Reversal initiation failed: ${reversalResult.message}`
                });
                return { success: false, status: 'REVERSAL_INITIATION_FAILED', error: reversalResult.message };
            }
        }
    } catch (error) {
        logger.error(`❌ CRITICAL ERROR during Airtime Fulfillment for TransID ${transactionId}:`, {
            message: error.message,
            stack: error.stack,
            sourceCallbackData: sourceCallbackData,
            requestType: requestType,
        });

        // Ensure main transaction record reflects critical error
        if (transactionId) {
            try {
                await transactionsCollection.doc(transactionId).update({
                    status: 'CRITICAL_FULFILLMENT_ERROR',
                    errorMessage: `Critical server error during airtime fulfillment: ${error.message}`,
                    lastUpdated: now,
                });
            } catch (updateError) {
                logger.error(`❌ Failed to update transaction ${transactionId} after critical fulfillment error:`, updateError.message);
            }
        }

        // Add to errors collection as a fallback
        await errorsCollection.add({
            type: 'CRITICAL_FULFILLMENT_ERROR',
            error: error.message,
            stack: error.stack,
            transactionId: transactionId,
            requestType: requestType,
            sourceCallbackData: sourceCallbackData,
            createdAt: now,
        });

        return { success: false, status: 'CRITICAL_ERROR', error: error.message };
    }
}



// --- RATE LIMITING ---
const stkPushLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 20, // Limit each IP to 20 requests per window
    message: 'Too many STK Push requests from this IP, please try again after a minute.',
    statusCode: 429,
    headers: true,
});

const stkCallbackRateLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 100, // M-Pesa can send multiple retries
    message: 'Too many STK Callback requests, please try again later.',
    statusCode: 429,
    headers: true,
});


// --- ENDPOINTS ---

// 1. STK Push Initiation Endpoint
app.post('/stk-push', stkPushLimiter, async (req, res) => {
    const { amount, phoneNumber, recipient } = req.body; // recipient is the number to top up

    if (!amount || !phoneNumber || !recipient) {
        logger.warn('Missing required parameters for STK Push:', { amount, phoneNumber, recipient });
        return res.status(400).json({ success: false, message: 'Missing required parameters: amount, phoneNumber, recipient.' });
    }

    const timestamp = generateTimestamp();
    const password = generatePassword(SHORTCODE, PASSKEY, timestamp);

    logger.info(`Initiating STK Push for recipient: ${recipient}, amount: ${amount}, customer: ${phoneNumber}`);

    // --- Input Validation (moved here for early exit) ---
    const MIN_AMOUNT = 5;
    const MAX_AMOUNT = 5000;
    const amountFloat = parseFloat(amount);

    if (isNaN(amountFloat) || amountFloat < MIN_AMOUNT || amountFloat > MAX_AMOUNT) {
        logger.warn(`🛑 Invalid amount ${amount} for STK Push. Amount must be between ${MIN_AMOUNT} and ${MAX_AMOUNT}.`);
        return res.status(400).json({ success: false, message: `Invalid amount. Must be between ${MIN_AMOUNT} and ${MAX_AMOUNT}.` });
    }

    const cleanedRecipient = recipient.replace(/\D/g, ''); // Ensure only digits
    const cleanedCustomerPhone = phoneNumber.replace(/\D/g, ''); // Ensure only digits

    if (!cleanedRecipient || !cleanedCustomerPhone || cleanedRecipient.length < 9 || cleanedCustomerPhone.length < 9) {
        logger.warn(`🛑 Invalid recipient (${recipient}) or customer phone (${phoneNumber}) for STK Push.`);
        return res.status(400).json({ success: false, message: "Invalid recipient or customer phone number format." });
    }

    const detectedCarrier = detectCarrier(cleanedRecipient); // Detect carrier at initiation
    if (detectedCarrier === 'Unknown') {
        logger.warn(`🛑 Unknown carrier for recipient ${cleanedRecipient}.`);
        return res.status(400).json({ success: false, message: "Recipient's carrier is not supported." });
    }

    try {
        const accessToken = await getAccessToken();

        const stkPushPayload = {
            BusinessShortCode: SHORTCODE,
            Password: password,
            Timestamp: timestamp,
            TransactionType: 'CustomerPayBillOnline', // Or 'CustomerBuyGoodsOnline' if applicable
            Amount: amountFloat, // Use the parsed float amount
            PartyA: cleanedCustomerPhone, // Customer's phone number
            PartyB: SHORTCODE, // Your Paybill/Till number
            PhoneNumber: cleanedCustomerPhone, // Customer's phone number
            CallBackURL: STK_CALLBACK_URL,
            AccountReference: cleanedRecipient, // Use recipient number as account reference
            TransactionDesc: `Airtime for ${cleanedRecipient}`
        };

        const stkPushResponse = await axios.post(
            'https://api.safaricom.co.ke/mpesa/stkpush/v1/processrequest',
            stkPushPayload,
            {
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json' // Explicitly set Content-Type
                }
            }
        );

        logger.info('STK Push Request Sent to Daraja:', stkPushResponse.data);

        const {
            ResponseCode,
            ResponseDescription,
            CustomerMessage,
            CheckoutRequestID, // This is the ID M-Pesa provides
            MerchantRequestID
        } = stkPushResponse.data;

        // ONLY create the sales and transaction documents if M-Pesa successfully accepted the push request
        if (ResponseCode === '0') {
            await stkTransactionsCollection.doc(CheckoutRequestID).set({
                checkoutRequestID: CheckoutRequestID,
                merchantRequestID: null, // Will be updated by callback
                phoneNumber: normalizedPhoneNumber, // The number that received the STK Push
                amount: amount,
                recipient: recipient, // Crucial: Store the intended recipient here
                carrier: carrier, // Assuming you detect carrier during initial request
                initialRequestAt: FieldValue.serverTimestamp(),
                stkPushStatus: 'PUSH_INITIATED', // Initial status
                stkPushPayload: stkPushPayload, // Store the payload sent to Daraja
                customerName: customerName || null,
                serviceType: serviceType || 'airtime',
                reference: reference || null,
            // You can add other fields here that are specific to your STK request
        });
            logger.info(`✅ STK Transaction document ${CheckoutRequestID} created with STK Push initiation response.`);
            
            return res.status(200).json({ success: true, message: CustomerMessage, checkoutRequestID: CheckoutRequestID });

        } else {
            // M-Pesa did not accept the push request (e.g., invalid number, insufficient balance in your shortcode)
            logger.error('❌ STK Push Request Failed by Daraja:', stkPushResponse.data);

            // Log this failure in errors collection
            await errorsCollection.add({
                type: 'STK_PUSH_INITIATION_FAILED_BY_DARJA',
                error: ResponseDescription,
                requestPayload: stkPushPayload,
                mpesaResponse: stkPushResponse.data,
                createdAt: FieldValue.serverTimestamp(),
            });

            // No sales/transaction documents created as M-Pesa rejected the request
            return res.status(500).json({ success: false, message: ResponseDescription || 'STK Push request failed.' });
        }

    } catch (error) {
        logger.error('❌ Critical error during STK Push initiation:', {
            message: error.message,
            stack: error.stack,
            requestBody: req.body,
            responseError: error.response ? error.response.data : 'No response data' // Log M-Pesa's error response if available
        });

        const errorMessage = error.response ? (error.response.data.errorMessage || error.response.data.MpesaError || error.response.data) : error.message;

        // Log the error
        await errorsCollection.add({
            type: 'STK_PUSH_CRITICAL_INITIATION_ERROR',
            error: errorMessage,
            requestBody: req.body,
            stack: error.stack,
            createdAt: FieldValue.serverTimestamp(),
        });

        res.status(500).json({ success: false, message: 'Failed to initiate STK Push.', error: errorMessage });
    }
});

// 2. M-Pesa STK Callback Endpoint (where M-Pesa sends payment confirmation)
/*app.post('/stk-callback', stkCallbackRateLimiter, async (req, res) => {
    const callback = req.body;
    const now = FieldValue.serverTimestamp();

    logger.info('📞 Received STK Callback:', JSON.stringify(callback));

    const resultCode = callback.Body.stkCallback.ResultCode;
    const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;

    // Extract details without optional chaining (as per previous regeneration)
    let mpesaReceiptNumber = null;
    let transactionDateFromMpesa = null;
    let phoneNumberUsedForPayment = null;
    let amountPaidByCustomer = null;

    const callbackMetadata = callback.Body.stkCallback.CallbackMetadata;
    if (callbackMetadata && Array.isArray(callbackMetadata.Item)) {
        const receiptItem = callbackMetadata.Item.find(item => item.Name === 'MpesaReceiptNumber');
        if (receiptItem) {
            mpesaReceiptNumber = receiptItem.Value;
        }
        const dateItem = callbackMetadata.Item.find(item => item.Name === 'TransactionDate');
        if (dateItem) {
            transactionDateFromMpesa = dateItem.Value;
        }
        const phoneItem = callbackMetadata.Item.find(item => item.Name === 'PhoneNumber');
        if (phoneItem) {
            phoneNumberUsedForPayment = phoneItem.Value;
        }
        const amountItem = callbackMetadata.Item.find(item => item.Name === 'Amount');
        if (amountItem) {
            amountPaidByCustomer = amountItem.Value;
        }
    }

    // Retrieve the initial sales request document
    const initialSalesRequestDocRef = salesCollection.doc(checkoutRequestID); // For STK, CheckoutRequestID is the saleId
    const initialSalesRequestDoc = await initialSalesRequestDocRef.get();

    if (!initialSalesRequestDoc.exists) {
        logger.error('❌ No matching initial sales request for CheckoutRequestID in Firestore:', checkoutRequestID);
        await errorsCollection.doc(`STK_CALLBACK_NO_INITIAL_REQUEST_${checkoutRequestID}_${Date.now()}`).set({
            type: 'STK_CALLBACK_ERROR',
            error: 'No matching initial sales request found for CheckoutRequestID. Cannot process callback.',
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });
        return res.json({ ResultCode: 0, ResultDesc: 'Callback received, but initial transaction request not found locally.' });
    }

    const initialRequestData = initialSalesRequestDoc.data();
    const topupNumber = initialRequestData.recipient; // Get recipient from initial request
    const originalInitiatorPhoneNumber = initialRequestData.initiatorPhoneNumber; // The phone number that initiated STK Push

    // Reference to the 'transactions' document (using checkoutRequestID as its ID)
    const transactionDocRef = transactionsCollection.doc(checkoutRequestID);

    if (resultCode === 0) {
        // M-Pesa payment was successful
        logger.info(`✅ M-Pesa payment successful for ${checkoutRequestID}. Marking for fulfillment.`);

        // Create or update the transaction record with initial details
        await transactionDocRef.set({
            transactionID: checkoutRequestID,
            type: 'STK_PUSH_PAYMENT',
            transactionTime: transactionDateFromMpesa,
            amountReceived: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
            mpesaReceiptNumber: mpesaReceiptNumber,
            payerMsisdn: phoneNumberUsedForPayment,
            billRefNumber: topupNumber, // The recipient number for airtime
            carrier: initialRequestData.carrier, // Get carrier from initial request
            mpesaRawCallback: callback,
            status: 'RECEIVED_PENDING_FULFILLMENT', // Set to pending fulfillment
            fulfillmentStatus: 'PENDING', // Initial fulfillment status
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            createdAt: initialRequestData.createdAt || now,
            lastUpdated: now,
            relatedSaleId: checkoutRequestID, // Link to the existing sales request doc
        }, { merge: true });
        logger.info(`✅ [transactions] Record for ${checkoutRequestID} created/updated with status: RECEIVED_PENDING_FULFILLMENT.`);

        // --- Trigger the unified airtime fulfillment process ---
        const fulfillmentResult = await processAirtimeFulfillment({
            transactionId: checkoutRequestID,
            originalAmountPaid: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : 0,
            payerMsisdn: phoneNumberUsedForPayment,
            payerName: initialRequestData.payerName || null, // If you capture name at initiation
            topupNumber: topupNumber,
            sourceCallbackData: callback,
            requestType: 'STK_PUSH',
            relatedSaleId: checkoutRequestID, // Pass the already created saleId
        });

        logger.info(`STK Callback for CheckoutRequestID ${checkoutRequestID} completed. Fulfillment Result:`, fulfillmentResult);

    } else {
        // M-Pesa payment failed or was cancelled by user
        logger.info(`❌ Payment failed for ${checkoutRequestID}. ResultCode: ${resultCode}, Desc: ${callback.Body.stkCallback.ResultDesc}`);
        const finalTransactionStatus = 'MPESA_PAYMENT_FAILED';

        await transactionDocRef.set({
            transactionID: checkoutRequestID,
            type: 'STK_PUSH_PAYMENT',
            transactionTime: transactionDateFromMpesa,
            amountReceived: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
            mpesaReceiptNumber: mpesaReceiptNumber,
            payerMsisdn: phoneNumberUsedForPayment,
            billRefNumber: topupNumber,
            carrier: initialRequestData.carrier,
            mpesaRawCallback: callback,
            status: finalTransactionStatus,
            fulfillmentStatus: 'NOT_APPLICABLE',
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            errorMessage: `M-Pesa payment failed: ${callback.Body.stkCallback.ResultDesc}`,
            createdAt: initialRequestData.createdAt || now,
            lastUpdated: now,
            relatedSaleId: checkoutRequestID,
            reconciliationNeeded: false,
        }, { merge: true });
        logger.info(`✅ [transactions] Record for ${checkoutRequestID} updated to: ${finalTransactionStatus}`);

        await errorsCollection.doc(`STK_PAYMENT_FAILED_${checkoutRequestID}`).set({
            type: 'STK_PAYMENT_ERROR',
            error: `STK Payment failed or was cancelled. ResultCode: ${resultCode}, ResultDesc: ${callback.Body.stkCallback.ResultDesc}`,
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });

        // Update the initial 'sales' request document to reflect the payment status
        await initialSalesRequestDocRef.update({
            mpesaPaymentStatus: finalTransactionStatus,
            mpesaReceiptNumber: mpesaReceiptNumber,
            mpesaTransactionDate: transactionDateFromMpesa,
            mpesaPhoneNumberUsed: phoneNumberUsedForPayment,
            mpesaAmountPaid: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
            fullStkCallback: callback,
            lastUpdated: now,
            // No fulfillment-related fields updated here as payment failed
        });
        logger.info(`✅ [sales/initial_request] M-Pesa payment status for ${checkoutRequestID} updated to ${finalTransactionStatus}.`);
    }

    res.json({ ResultCode: 0, ResultDesc: 'Callback received and payment status recorded by DaimaPay server.' });
});
*/
// Modified STK Callback Endpoint
app.post('/stk-callback', async (req, res) => {
    const callback = req.body;
    logger.info('📞 Received STK Callback:', JSON.stringify(callback, null, 2)); // Log full callback for debugging

    // Safaricom sends an empty object on initial push confirmation before payment
    if (!callback || !callback.Body || !callback.Body.stkCallback) {
        logger.warn('Received an empty or malformed STK callback. Ignoring.');
        return res.json({ ResultCode: 0, ResultDesc: 'Callback processed (ignored empty/malformed).' });
    }

    const { MerchantRequestID, CheckoutRequestID, ResultCode, ResultDesc, CallbackMetadata } = callback.Body.stkCallback;

    // Extracting relevant data for logging and processing
    const amount = CallbackMetadata?.Item.find(item => item.Name === 'Amount')?.Value;
    const mpesaReceiptNumber = CallbackMetadata?.Item.find(item => item.Name === 'MpesaReceiptNumber')?.Value;
    const transactionDate = CallbackMetadata?.Item.find(item => item.Name === 'TransactionDate')?.Value;
    const phoneNumber = CallbackMetadata?.Item.find(item => item.Name === 'PhoneNumber')?.Value;

    // Retrieve the initial sales request document
    const initialSalesRequestDocRef = salesCollection.doc(CheckoutRequestID);
    const initialSalesRequestDoc = await initialSalesRequestDocRef.get();

    if (!initialSalesRequestDoc.exists) {
        logger.error('❌ No matching initial sales request for CheckoutRequestID in Firestore:', CheckoutRequestID);
        // Respond with success to M-Pesa to prevent retries of this unknown callback
        return res.json({ ResultCode: 0, ResultDesc: 'No matching transaction found.' });
    }

    const initialSaleData = initialSalesRequestDoc.data();

    // Check M-Pesa ResultCode for success
    if (ResultCode === 0) {
        logger.info(`✅ M-Pesa payment successful for ${CheckoutRequestID}. Marking for external fulfillment.`);

        const updatedSalesData = {
            mpesaPaymentStatus: 'SUCCESSFUL_PENDING_EXTERNAL_FULFILLMENT', // New status
            mpesaResultCode: ResultCode,
            mpesaResultDesc: ResultDesc,
            mpesaReceiptNumber: mpesaReceiptNumber,
            mpesaTransactionDate: transactionDate,
            customerPhoneNumber: phoneNumber,
            amountPaid: amount,
            mpesaCallbackMetadata: CallbackMetadata, // Store full metadata
            lastUpdated: FieldValue.serverTimestamp(),
        };

        const updatedTransactionData = {
            status: 'RECEIVED_PENDING_OFFLINE_FULFILLMENT', // New status for transactions collection
            mpesaPaymentStatus: 'SUCCESSFUL',
            mpesaCallbackData: callback.Body.stkCallback,
            amountConfirmed: amount,
            mpesaReceiptNumber: mpesaReceiptNumber,
            mpesaTransactionDate: transactionDate,
            lastUpdated: FieldValue.serverTimestamp(),
        };

        try {
            await initialSalesRequestDocRef.update(updatedSalesData);
            logger.info(`✅ Sales document ${CheckoutRequestID} updated with SUCCESSFUL_PENDING_EXTERNAL_FULFILLMENT status.`);

            const transactionDocRef = transactionsCollection.doc(CheckoutRequestID);
            await transactionDocRef.update(updatedTransactionData);
            logger.info(`✅ Transaction document ${CheckoutRequestID} updated with status: RECEIVED_PENDING_OFFLINE_FULFILLMENT.`);

            // --- NOTIFY OFFLINE SERVER FOR FULFILLMENT ---
            const fulfillmentDetails = {
                checkoutRequestID: CheckoutRequestID,
                merchantRequestID: MerchantRequestID,
                mpesaReceiptNumber: mpesaReceiptNumber,
                amountPaid: amount,
                recipientNumber: initialSaleData.recipient, 
                customerPhoneNumber: phoneNumber, 
                carrier: initialSaleData.carrier, 
            };
            const notificationResult = await notifyOfflineServerForFulfillment(fulfillmentDetails);

            if (notificationResult.success) {
                logger.info(`✅ Offline server successfully notified for fulfillment of ${CheckoutRequestID}.`);
                await initialSalesRequestDocRef.update({
                    offlineNotificationStatus: 'SUCCESS',
                    lastUpdated: FieldValue.serverTimestamp(),
                });
            } else {
                logger.error(`❌ Failed to notify offline server for ${CheckoutRequestID}. Reversal might be needed manually if fulfillment not picked up.`);
                await initialSalesRequestDocRef.update({
                    offlineNotificationStatus: 'FAILED',
                    offlineNotificationError: notificationResult.message,
                    lastUpdated: FieldValue.serverTimestamp(),
                });
            }

            // Respond to M-Pesa that the callback was processed
            return res.json({ ResultCode: 0, ResultDesc: 'Callback received and processing for external fulfillment initiated.' });

        } catch (updateError) {
            logger.error(`❌ Error updating Firestore or notifying offline server for ${CheckoutRequestID}:`, { message: updateError.message, stack: updateError.stack });
            await errorsCollection.add({
                type: 'STK_CALLBACK_FIRESTORE_UPDATE_OR_NOTIFICATION_ERROR',
                checkoutRequestID: CheckoutRequestID,
                error: updateError.message,
                stack: updateError.stack,
                callbackData: callback,
                createdAt: FieldValue.serverTimestamp(),
            });
            // Still respond success to M-Pesa to prevent retries (you'll handle the error internally)
            return res.json({ ResultCode: 0, ResultDesc: 'Callback processed with internal error during update/notification.' });
        }

    } else {
        // M-Pesa payment failed or was cancelled by user
        logger.warn(`⚠️ M-Pesa payment failed or cancelled for ${CheckoutRequestID}. ResultCode: ${ResultCode}, ResultDesc: ${ResultDesc}`);

        const updatedSalesData = {
            mpesaPaymentStatus: 'FAILED_OR_CANCELLED',
            mpesaResultCode: ResultCode,
            mpesaResultDesc: ResultDesc,
            customerPhoneNumber: phoneNumber,
            mpesaCallbackMetadata: CallbackMetadata,
            lastUpdated: FieldValue.serverTimestamp(),
        };

        const updatedTransactionData = {
            status: 'PAYMENT_FAILED_OR_CANCELLED',
            mpesaPaymentStatus: 'FAILED',
            mpesaCallbackData: callback.Body.stkCallback,
            lastUpdated: FieldValue.serverTimestamp(),
        };

        try {
            await initialSalesRequestDocRef.update(updatedSalesData);
            await transactionsCollection.doc(CheckoutRequestID).update(updatedTransactionData);
            logger.info(`✅ Sales and transaction documents updated for failed/cancelled payment for ${CheckoutRequestID}.`);
        } catch (error) {
            logger.error(`❌ Error updating documents for failed/cancelled STK payment ${CheckoutRequestID}:`, { message: error.message, stack: error.stack });
            await errorsCollection.add({
                type: 'STK_CALLBACK_FAILED_PAYMENT_UPDATE_ERROR',
                checkoutRequestID: CheckoutRequestID,
                error: error.message,
                stack: error.stack,
                callbackData: callback,
                createdAt: FieldValue.serverTimestamp(),
            });
        }
        // Always respond with success to M-Pesa even for failed payments, to acknowledge receipt of the callback.
        return res.json({ ResultCode: 0, ResultDesc: 'Payment failed/cancelled. Callback processed.' });
    }
});
// Daraja Reversal Result Endpoint
app.post('/daraja-reversal-result', async (req, res) => {
    try {
        const result = req.body?.Result;
        logger.info('📞 Received Daraja Reversal Result Callback:', result);

        const resultCode = result?.ResultCode;
        const resultDesc = result?.ResultDesc;
        const reversalTransactionId = result?.TransactionID;

        const params = result?.ResultParameters?.ResultParameter || [];

        // Extract parameters safely
        const extractParam = (key) => params.find(p => p.Key === key)?.Value;

        const originalTransactionId = extractParam('OriginalTransactionID');
        const amount = extractParam('Amount');
        const creditParty = extractParam('CreditPartyPublicName');
        const debitParty = extractParam('DebitPartyPublicName');

        if (!originalTransactionId) {
            logger.error("❌ Missing OriginalTransactionID in reversal callback", { rawCallback: req.body });
            return res.status(400).json({ ResultCode: 0, ResultDesc: "Missing OriginalTransactionID. Logged for manual review." });
        }

        const transactionRef = transactionsCollection.doc(originalTransactionId);
        const transactionDoc = await transactionRef.get();

        if (!transactionDoc.exists) {
            logger.warn(`⚠️ Reversal result received for unknown OriginalTransactionID: ${originalTransactionId}`);
            return res.json({ ResultCode: 0, ResultDesc: "Acknowledged - Unknown transaction." });
        }

        if (resultCode === 0) {
            logger.info(`✅ Reversal for TransID ${originalTransactionId} COMPLETED successfully.`);
            await transactionRef.update({
                status: 'REVERSED_SUCCESSFULLY',
                reversalConfirmationDetails: result,
                lastUpdated: FieldValue.serverTimestamp(),
            });
            await reconciledTransactionsCollection.doc(originalTransactionId).update({
                status: 'REVERSAL_CONFIRMED',
                reversalConfirmationDetails: result,
                lastUpdated: FieldValue.serverTimestamp(),
            });
        } else {
            logger.error(`❌ Reversal for TransID ${originalTransactionId} FAILED: ${resultDesc}`);
            await transactionRef.update({
                status: 'REVERSAL_FAILED_CONFIRMATION',
                reversalConfirmationDetails: result,
                errorMessage: `Reversal failed: ${resultDesc}`,
                lastUpdated: FieldValue.serverTimestamp(),
            });
            await failedReconciliationsCollection.doc(originalTransactionId).set({
                transactionId: originalTransactionId,
                reversalConfirmationDetails: result,
                reason: resultDesc,
                createdAt: FieldValue.serverTimestamp(),
            }, { merge: true });
        }

        res.json({ ResultCode: 0, ResultDesc: "Reversal result processed successfully." });

    } catch (error) {
        logger.error("❌ Error processing Daraja reversal callback", {
            message: error.message,
            stack: error.stack,
            rawBody: req.body,
        });
        res.status(500).json({ ResultCode: 0, ResultDesc: "Server error during reversal processing." });
    }
});


// --- Daraja Reversal Queue Timeout Endpoint ---
app.post('/daraja-reversal-timeout', async (req, res) => {
    const timeoutData = req.body;
    const now = new Date();
    const { OriginatorConversationID, ConversationID, ResultCode, ResultDesc } = timeoutData;

    logger.warn('⚠️ Received Daraja Reversal Queue Timeout Callback:', {
        OriginatorConversationID: OriginatorConversationID,
        ConversationID: ConversationID,
        ResultCode: ResultCode,
        ResultDesc: ResultDesc,
        fullCallback: timeoutData
    });

    try {
        let transactionIdToUpdate = OriginatorConversationID;

        const originalTransactionRef = transactionsCollection.doc(transactionIdToUpdate);
        const originalTransactionDoc = await originalTransactionRef.get();

        if (originalTransactionDoc.exists) {
            logger.info(`Updating transaction ${transactionIdToUpdate} with reversal timeout status.`);
            await originalTransactionRef.update({
                status: 'REVERSAL_TIMED_OUT', // New status for timed-out reversals
                reversalTimeoutDetails: timeoutData,
                lastUpdated: FieldValue.serverTimestamp(),
            });
        } else {
            logger.warn(`⚠️ Reversal Timeout received for unknown or unlinked TransID/OriginatorConversationID: ${transactionIdToUpdate}`);
        }

        // Always record the timeout in a dedicated collection for auditing/manual review
        await reversalTimeoutsCollection.add({
            transactionId: transactionIdToUpdate, // The ID you're tracking internally
            originatorConversationId: OriginatorConversationID,
            conversationId: ConversationID,
            resultCode: ResultCode,
            resultDesc: ResultDesc,
            fullCallbackData: timeoutData,
            createdAt: FieldValue.serverTimestamp(),
        });

        logger.info(`✅ Daraja Reversal Queue Timeout processed for ${transactionIdToUpdate}.`);
        res.json({ "ResultCode": 0, "ResultDesc": "Daraja Reversal Queue Timeout Received and Processed." });

    } catch (error) {
        logger.error(`❌ CRITICAL ERROR processing Daraja Reversal Queue Timeout for ${OriginatorConversationID || 'N/A'}:`, {
            message: error.message,
            stack: error.stack,
            timeoutData: timeoutData
        });
        // Still send a success response to Daraja to avoid repeated callbacks
        res.json({ "ResultCode": 0, "ResultDesc": "Internal server error during Queue Timeout processing." });
    }
});
        
// --- NEW AIRTIME BONUS API ENDPOINTS ---
const CURRENT_BONUS_DOC_PATH = 'airtime_bonuses/current_settings'; // Document path for current settings
// BONUS_HISTORY_COLLECTION is already defined at the top as a const

// GET current bonus percentages
app.get('/api/airtime-bonuses/current', async (req, res) => {
    try {
        const docRef = firestore.collection('airtime_bonuses').doc('current_settings');
        const docSnap = await docRef.get();

        if (docSnap.exists) {
            res.json(docSnap.data());
        } else {
            // If document doesn't exist, initialize it with default values
            logger.info('Initializing airtime_bonuses/current_settings with default values.');
            await docRef.set({ safaricomPercentage: 0, africastalkingPercentage: 0, lastUpdated: FieldValue.serverTimestamp() });
            res.json({ safaricomPercentage: 0, africastalkingPercentage: 0 });
        }
    } catch (error) {
        logger.error('Error fetching current airtime bonuses:', { message: error.message, stack: error.stack });
        res.status(500).json({ error: 'Failed to fetch current airtime bonuses.' });
    }
});

// POST to update bonus percentages and log history
app.post('/api/airtime-bonuses/update', async (req, res) => {
    const { safaricomPercentage, africastalkingPercentage, actor } = req.body; // 'actor' could be the authenticated user's ID/email

    if (typeof safaricomPercentage !== 'number' || typeof africastalkingPercentage !== 'number' || safaricomPercentage < 0 || africastalkingPercentage < 0) {
        logger.warn('Invalid bonus percentages received for update.', { safaricomPercentage, africastalkingPercentage });
        return res.status(400).json({ error: 'Invalid bonus percentages. Must be non-negative numbers.' });
    }

    try {
        const currentSettingsDocRef = firestore.collection('airtime_bonuses').doc('current_settings');
        const currentSettingsSnap = await currentSettingsDocRef.get();
        const oldSettings = currentSettingsSnap.exists ? currentSettingsSnap.data() : { safaricomPercentage: 0, africastalkingPercentage: 0 };

        const batch = firestore.batch();

        // Update the current settings document
        batch.set(currentSettingsDocRef, {
            safaricomPercentage: safaricomPercentage,
            africastalkingPercentage: africastalkingPercentage,
            lastUpdated: FieldValue.serverTimestamp(), // Use server timestamp
        }, { merge: true }); // Use merge to avoid overwriting other fields if they exist

        // Add history entries only if values have changed
        if (safaricomPercentage !== oldSettings.safaricomPercentage) {
            batch.set(bonusHistoryCollection.doc(), { // Use the initialized collection variable
                company: 'Safaricom',
                oldPercentage: oldSettings.safaricomPercentage || 0,
                newPercentage: safaricomPercentage,
                timestamp: FieldValue.serverTimestamp(),
                actor: actor || 'system', // Default to 'system' if actor is not provided
            });
            logger.info(`Safaricom bonus changed from ${oldSettings.safaricomPercentage} to ${safaricomPercentage} by ${actor || 'system'}.`);
        }
        if (africastalkingPercentage !== oldSettings.africastalkingPercentage) {
            batch.set(bonusHistoryCollection.doc(), { // Use the initialized collection variable
                company: 'AfricasTalking',
                oldPercentage: oldSettings.africastalkingPercentage || 0,
                newPercentage: africastalkingPercentage,
                timestamp: FieldValue.serverTimestamp(),
                actor: actor || 'system', // Default to 'system' if actor is not provided
            });
            logger.info(`AfricasTalking bonus changed from ${oldSettings.africastalkingPercentage} to ${africastalkingPercentage} by ${actor || 'system'}.`);
        }

        await batch.commit();
        res.json({ success: true, message: 'Bonus percentages updated successfully.' });

    } catch (error) {
        logger.error('Error updating airtime bonuses:', { message: error.message, stack: error.stack }); // Completed the error message
        res.status(500).json({ error: 'Failed to update airtime bonuses.' });
    }
});

app.get("/ping", (req, res) => {
  res.status(200).send("pong");
});

// --- Start the Server ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    logger.info(`🚀 Server running on port ${PORT}`);
});
