require('dotenv').config(); 
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const rateLimit = require('express-rate-limit');
const admin = require('firebase-admin');
const AfricasTalking = require('africastalking'); 

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_DATABASE_URL 
});

const db = admin.firestore();
const transactionsCollection = db.collection('transactions');
const salesCollection = db.collection('sales');
const errorsCollection = db.collection('errors');
const failedReconciliationsCollection = db.collection('failed_reconciliations');
const bonusHistoryCollection = db.collection('bonus_history');
const airtimeBonusesDocRef = db.collection('airtime_bonuses').doc('current_settings');
const safaricomDealerConfigRef = db.collection('mpesa_settings').doc('main_config');

const app = express();
app.use(bodyParser.json());

// --- Simple Logger Utility ---
const logger = {
    info: (...args) => console.log(`[INFO] [${new Date().toISOString()}]`, ...args),
    warn: (...args) => console.warn(`[WARN] [${new Date().toISOString()}]`, ...args),
    error: (...args) => console.error(`[ERROR] [${new Date().toISOString()}]`, ...args),
    debug: (...args) => process.env.NODE_ENV !== 'production' && console.log(`[DEBUG] [${new Date().toISOString()}]`, ...args), // Debug only in non-prod
};

// --- Configuration Constants from Environment Variables ---
const PORT = process.env.PORT || 3000;
const BUSINESS_SHORT_CODE = process.env.BUSINESS_SHORT_CODE;
const PASSKEY = process.env.PASSKEY;
const CONSUMER_KEY = process.env.CONSUMER_KEY; // For M-Pesa STK Push Auth
const CONSUMER_SECRET = process.env.CONSUMER_SECRET; // For M-Pesa STK Push Auth
const CALLBACK_URL = process.env.CALLBACK_URL; // Your publicly accessible URL for M-Pesa callbacks
const ANALYTICS_SERVER_URL = process.env.ANALYTICS_SERVER_URL; 

// NEW: Environment variables for Safaricom Dealer API and Africa's Talking
const SAFARICOM_MPESA_AUTH_URL = process.env.MPESA_AUTH_URL; 
const SAFARICOM_STK_PUSH_URL = process.env.MPESA_STK_PUSH_URL; 

const MPESA_AIRTIME_KEY = process.env.MPESA_AIRTIME_KEY; 
const MPESA_AIRTIME_SECRET = process.env.MPESA_AIRTIME_SECRET; 
const MPESA_GRANT_URL = process.env.MPESA_GRANT_URL; 
const DEALER_SENDER_MSISDN = process.env.DEALER_SENDER_MSISDN; 
const MPESA_AIRTIME_URL = process.env.MPESA_AIRTIME_URL; 

const AT_API_KEY = process.env.AT_API_KEY; 
const AT_USERNAME = process.env.AT_USERNAME; 

// Initialize Africa's Talking SDK
const africastalking = AfricasTalking({
    apiKey: AT_API_KEY,
    username: AT_USERNAME,
});


// --- Cache variables for Safaricom Airtime Token ---
let cachedAirtimeToken = null;
let tokenExpiryTimestamp = 0;

// NEW: Cache variables for Dealer Service PIN
let cachedDealerServicePin = null;
let dealerPinExpiryTimestamp = 0;
const DEALER_PIN_CACHE_TTL = 10 * 60 * 1000; 

// --- Helper Functions ---
async function getAccessToken() {
    const auth = Buffer.from(`${CONSUMER_KEY}:${CONSUMER_SECRET}`).toString('base64');
    try {
        const response = await axios.get(SAFARICOM_MPESA_AUTH_URL, {
            headers: {
                'Authorization': `Basic ${auth}`
            }
        });
        return response.data.access_token;
    } catch (error) {
        logger.error('Error getting M-Pesa access token (STK Push):', error.message);
        throw new Error('Failed to get M-Pesa access token.');
    }
}

// NEW: Service PIN generation
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


// NEW: Function to get Safaricom Dealer API access token with caching
async function getCachedAirtimeToken() {
    const now = Date.now();
    if (cachedAirtimeToken && now < tokenExpiryTimestamp) {
        logger.info('🔑 Using cached dealer token.');
        return cachedAirtimeToken;
    }
    try {
        const auth = Buffer.from(`${MPESA_AIRTIME_KEY}:${MPESA_AIRTIME_SECRET}`).toString('base64');
        const response = await axios.post(
            MPESA_GRANT_URL,
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

// NEW: Carrier detection helper
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

// NEW: Normalize phone number for Safaricom Dealer API (7XXXXXXXX format)
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

// NEW: Send Safaricom dealer airtime
async function sendSafaricomAirtime(receiverNumber, amount) {
    try {
        const token = await getCachedAirtimeToken();
        const normalizedReceiver = normalizeReceiverPhoneNumber(receiverNumber);
        const adjustedAmount = Math.round(amount * 100); // Amount in cents

        if (!DEALER_SENDER_MSISDN || !MPESA_AIRTIME_URL) {
            const missingEnvError = 'Missing Safaricom Dealer API environment variables (DEALER_SENDER_MSISDN, MPESA_AIRTIME_URL). DEALER_SERVICE_PIN is now fetched from Firestore.';
            logger.error(missingEnvError);
            return { status: 'FAILED', message: missingEnvError };
        }

        const rawDealerPin = await getDealerServicePin();
        const servicePin = await generateServicePin(rawDealerPin);

        const body = {
            senderMsisdn: DEALER_SENDER_MSISDN,
            amount: adjustedAmount,
            servicePin: servicePin,
            receiverMsisdn: normalizedReceiver,
        };

        const response = await axios.post(
            MPESA_AIRTIME_URL,
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

        const isSuccess = response.data && response.data.responseStatus === '200';

        if (response.data && response.data.responseDesc) {
            const desc = response.data.responseDesc;
            const idMatch = desc.match(/^(R\d{6}\.\d{4}\.\d{6})/); 
            if (idMatch && idMatch[1]) {
                safaricomInternalTransId = idMatch[1];
            }
            const balanceMatch = desc.match(/New balance is Ksh\. (\d+\.\d{2})/); 
            if (balanceMatch && balanceMatch[1]) {
                newSafaricomFloatBalance = parseFloat(balanceMatch[1]);
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
                error: response.data, 
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

// NEW: Function to send Africa's Talking Airtime
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

    if (!AT_API_KEY || !AT_USERNAME) {
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


// --- Routes ---

// STK Push initiation endpoint
app.post('/stk-push', async (req, res) => {
    const { phone_number, amount } = req.body; // Remove 'carrier' from req.body, detect it
    const now = admin.firestore.FieldValue.serverTimestamp();

    if (!phone_number || !amount) {
        return res.status(400).json({ success: false, message: 'Missing phone_number or amount.' });
    }

    // NEW: Detect carrier dynamically
    const carrier = detectCarrier(phone_number);
    if (carrier === 'Unknown') {
        logger.warn(`STK Push request with unknown carrier for phone number: ${phone_number}`);
        return res.status(400).json({ success: false, message: 'Unknown or unsupported carrier for provided phone number.' });
    }
    logger.info(`Detected carrier for ${phone_number}: ${carrier}`);

    try {
        const token = await getAccessToken(); // M-Pesa STK Push Token
        const timestamp = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
        const password = Buffer.from(`${BUSINESS_SHORT_CODE}${PASSKEY}${timestamp}`).toString('base64');
        const transactionRef = db.collection('transactions').doc(); // Auto-generate ID

        const stkPushPayload = {
            BusinessShortCode: BUSINESS_SHORT_CODE,
            Password: password,
            Timestamp: timestamp,
            TransactionType: 'CustomerPayBillOnline', 
            Amount: amount,
            PartyA: phone_number,
            PartyB: BUSINESS_SHORT_CODE,
            PhoneNumber: phone_number,
            CallBackURL: CALLBACK_URL,
            AccountReference: transactionRef.id, 
            TransactionDesc: `Airtime Top-up for ${phone_number} on ${carrier}`
        };

        logger.info('Sending STK Push with payload:', stkPushPayload);

        const response = await axios.post(SAFARICOM_STK_PUSH_URL, stkPushPayload, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });

        // Save initial transaction state to Firestore
        await transactionRef.set({
            checkoutRequestID: response.data.CheckoutRequestID,
            merchantRequestID: response.data.MerchantRequestID,
            responseCode: response.data.ResponseCode,
            responseDescription: response.data.ResponseDescription,
            customerMessage: response.data.CustomerMessage,
            status: 'PENDING_MPESA_PAYMENT', // Initial status
            recipient: phone_number,
            amount: parseFloat(amount),
            carrier: carrier, // Store detected carrier
            createdAt: now,
            lastUpdated: now,
            type: 'STK_PUSH_AIRTIME_TOPUP',
            reconciliationNeeded: false,
            primaryProviderUsed: null,
            secondaryProviderAttempted: false,
        });

        // Also save to sales collection with initial pending status
        await salesCollection.doc(response.data.CheckoutRequestID).set({
            checkoutRequestID: response.data.CheckoutRequestID,
            recipient: phone_number,
            amount: parseFloat(amount),
            carrier: carrier, // Store detected carrier
            status: 'PENDING_MPESA_PAYMENT',
            createdAt: now,
            lastUpdated: now,
            type: 'AIRTIME_SALE_ONLINE',
            mpesaResultCode: null,
            mpesaResultDesc: null,
            airtimeResult: null,
            bonus: 0,
            commission_rate: 0, 
            total_sent: parseFloat(amount), 
            reconciliationNeeded: false,
            providerUsed: null,
            secondaryProviderAttempted: false,
        });


        res.json({
            success: true,
            message: 'STK Push initiated successfully.',
            data: response.data,
            firestoreDocId: transactionRef.id 
        });

    } catch (error) {
        logger.error('Error initiating STK Push:', error.response ? error.response.data : error.message);

        // Log error to errorsCollection
        await errorsCollection.add({
            type: 'STK_PUSH_INITIATION_ERROR',
            error: error.response ? error.response.data : error.message,
            requestBody: req.body,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });

        res.status(500).json({
            success: false,
            message: 'Failed to initiate STK Push.',
            error: error.response ? error.response.data : error.message
        });
    }
});

const stkCallbackRateLimiter = rateLimit({
    windowMs: 5 * 60 * 1000, // 5 minutes
    max: 20, 
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many STK callbacks from this source. Try again later.' },
});

// M-Pesa STK Callback Handler
app.post('/stk-callback', stkCallbackRateLimiter, async (req, res) => {
    const callback = req.body;
    const now = admin.firestore.FieldValue.serverTimestamp();

    logger.info('📞 Received STK Callback:', JSON.stringify(callback));

    const resultCode = callback.Body.stkCallback.ResultCode;
    const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;
    const mpesaReceiptNumber = callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'MpesaReceiptNumber')?.Value || null;
    const transactionDateFromMpesa = callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'TransactionDate')?.Value || null;
    const phoneNumberUsedForPayment = callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'PhoneNumber')?.Value || null;
    const amountPaidByCustomer = callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'Amount')?.Value || null;

    const salesDocRef = salesCollection.doc(checkoutRequestID); 
    const salesDoc = await salesDocRef.get();

    if (!salesDoc.exists) {
        logger.error('❌ No matching transaction (sales collection) for CheckoutRequestID in Firestore:', checkoutRequestID);
        await errorsCollection.doc(`STK_CALLBACK_NO_TX_SALES_${checkoutRequestID}_${Date.now()}`).set({
            type: 'STK_CALLBACK_ERROR',
            error: 'No matching transaction found in salesCollection for CheckoutRequestID.',
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });
        return res.json({ resultCode: 0, resultDesc: 'No matching transaction found locally for this callback.' });
    }

    const txData = salesDoc.data(); 
    const { recipient: topupNumber, amount: originalAmount, carrier } = txData; 

    let finalTxStatus = 'FAILED';
    let finalSalesStatus = 'FAILED';
    let airtimeResult = null;
    let bonusAmount = 0; 
    let commissionRate = 0; 
    let finalAmountToDispatch = originalAmount; 
    let needsReconciliation = false;
    let providerUsed = null; 
    let secondaryProviderAttempted = false; 

    // Determine the status based on M-Pesa ResultCode
    if (resultCode === 0) {
        logger.info(`✅ M-Pesa payment successful for ${checkoutRequestID}. Attempting airtime dispatch.`);

        try {
            const bonusDocSnap = await airtimeBonusesDocRef.get(); 

            if (!bonusDocSnap.exists) {
                logger.warn('Bonus settings document does not exist (airtime_bonuses/current_settings). Skipping bonus application.');
            }

            const safaricomBonusPercentage = bonusDocSnap?.data()?.safaricomPercentage ?? 0;
            const atBonusPercentage = bonusDocSnap?.data()?.africastalkingPercentage ?? 0;


            // Custom rounding: 0.1–0.4 => 0, 0.5–0.9 => 1
            const customRound = (value) => {
                const decimalPart = value % 1;
                const integerPart = Math.floor(value);
                return decimalPart >= 0.5 ? integerPart + 1 : integerPart;
            };

            // Apply bonus with optional rounding
            const applyBonus = (percentage, label, round = false) => {
                const rawBonus = originalAmount * (percentage / 100);
                const bonus = round ? customRound(rawBonus) : parseFloat(rawBonus.toFixed(2)); // Ensure rawBonus is rounded to 2 decimal places for storage
                const total = originalAmount + bonus;
                logger.info(
                    `Applying ${percentage}% ${label} bonus. Original: ${originalAmount}, Bonus: ${bonus} (${round ? 'rounded' : 'raw'}), Final: ${total}`
                );
                return { total, bonus, percentage }; // Return percentage as commissionRate
            };

            // Normalize carrier name to lowercase
            const carrierNormalized = carrier?.toLowerCase();

            if (carrierNormalized === 'safaricom' && safaricomBonusPercentage > 0) {
                const result = applyBonus(safaricomBonusPercentage, 'Safaricom', false); // No rounding for Safaricom
                finalAmountToDispatch = result.total;
                bonusAmount = result.bonus;
                commissionRate = result.percentage;
            } else if (['airtel', 'telkom', 'equitel', 'faiba'].includes(carrierNormalized) && atBonusPercentage > 0) {
                const result = applyBonus(atBonusPercentage, 'AfricasTalking', true); // Use custom rounding for AT
                finalAmountToDispatch = result.total;
                bonusAmount = result.bonus;
                commissionRate = result.percentage;
            }

            logger.info(`Final amount to dispatch: ${finalAmountToDispatch}`);


            // --- Proceed with Airtime Dispatch (with finalAmountToDispatch) ---
            if (carrier === 'Safaricom') {
                // --- Primary attempt: Safaricom Dealer API for Safaricom numbers ---
                try {
                    airtimeResult = await sendSafaricomAirtime(topupNumber, finalAmountToDispatch); // Use finalAmountToDispatch
                    if (airtimeResult && airtimeResult.status === 'SUCCESS') { // Updated to check for 'SUCCESS'
                        finalTxStatus = 'COMPLETED';
                        finalSalesStatus = 'COMPLETED';
                        providerUsed = 'SafaricomDealerAPI';
                        logger.info(`✅ Safaricom airtime sent successfully via Safaricom Dealer API for ${checkoutRequestID}.`);
                    } else {
                        logger.warn(`⚠️ Safaricom Dealer API failed or returned non-SUCCESS status for ${checkoutRequestID}. Attempting Africa's Talking as fallback.`);
                        await errorsCollection.doc(`SAF_API_PRIMARY_FAIL_${checkoutRequestID}`).set({
                            type: 'AIRTIME_SEND_ERROR',
                            subType: 'SAFARICOM_PRIMARY_API_FAILURE',
                            error: `Safaricom API (primary) returned non-SUCCESS status: ${JSON.stringify(airtimeResult)}`,
                            transactionCode: checkoutRequestID,
                            originalAmount: originalAmount,
                            amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                            airtimeResponse: airtimeResult,
                            callbackData: callback,
                            createdAt: now,
                        });
                        secondaryProviderAttempted = true; // Mark that we are attempting fallback
                    }
                } catch (safPrimaryError) {
                    logger.error(`❌ Exception during Safaricom Dealer API call for ${checkoutRequestID}:`, safPrimaryError.message);
                    await errorsCollection.doc(`SAF_API_PRIMARY_EXCEPTION_${checkoutRequestID}`).set({
                        type: 'AIRTIME_SEND_ERROR',
                        subType: 'SAFARICOM_PRIMARY_API_EXCEPTION',
                        error: `Exception during Safaricom API (primary) call: ${safPrimaryError.message}`,
                        transactionCode: checkoutRequestID,
                        originalAmount: originalAmount,
                        amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                        stack: safPrimaryError.stack,
                        callbackData: callback,
                        createdAt: now,
                    });
                    secondaryProviderAttempted = true; // Mark that we are attempting fallback
                }

                // --- Secondary attempt: Africa's Talking for Safaricom numbers (if primary failed) ---
                if (finalSalesStatus !== 'COMPLETED' && secondaryProviderAttempted) { // Only attempt if primary failed
                    try {
                        logger.info(`Attempting Africa's Talking as fallback for Safaricom number ${topupNumber}...`);
                        airtimeResult = await sendAfricasTalkingAirtime(topupNumber, finalAmountToDispatch, carrier); // Use finalAmountToDispatch
                        if (airtimeResult && airtimeResult.status === 'SUCCESS') {
                            finalTxStatus = 'COMPLETED';
                            finalSalesStatus = 'COMPLETED';
                            providerUsed = 'AfricasTalking'; // Update to the one that succeeded
                            logger.info(`✅ Safaricom airtime sent successfully via Africa's Talking fallback for ${checkoutRequestID}.`);
                        } else {
                            logger.error(`❌ Africa's Talking fallback also failed for Safaricom number ${checkoutRequestID}:`, airtimeResult);
                            finalTxStatus = 'AIRTIME_FAILED_CUSTOMER_PAID'; // Specific status: Customer paid, but airtime failed
                            finalSalesStatus = 'AIRTIME_FAILED_CUSTOMER_PAID';
                            needsReconciliation = true; // Mark for reconciliation
                            await errorsCollection.doc(`SAF_AT_FALLBACK_FAIL_${checkoutRequestID}`).set({
                                type: 'AIRTIME_SEND_ERROR',
                                subType: 'AFRICASTALKING_FALLBACK_FAILURE',
                                error: `Africa's Talking fallback returned non-SUCCESS status or unsuccessful for Safaricom: ${JSON.stringify(airtimeResult)}`,
                                transactionCode: checkoutRequestID,
                                originalAmount: originalAmount,
                                amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                                airtimeResponse: airtimeResult,
                                callbackData: callback,
                                createdAt: now,
                            });
                        }
                    } catch (atFallbackError) {
                        logger.error(`❌ Exception during Africa's Talking fallback call for Safaricom number ${checkoutRequestID}:`, atFallbackError.message);
                        finalTxStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION'; // General runtime exception
                        finalSalesStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION';
                        needsReconciliation = true; // Mark for reconciliation
                        await errorsCollection.doc(`SAF_AT_FALLBACK_EXCEPTION_${checkoutRequestID}`).set({
                            type: 'AIRTIME_SEND_ERROR',
                            subType: 'AFRICASTALKING_FALLBACK_EXCEPTION',
                            error: `Exception during Africa's Talking fallback call for Safaricom: ${atFallbackError.message}`,
                            transactionCode: checkoutRequestID,
                            originalAmount: originalAmount,
                            amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                            stack: atFallbackError.stack,
                            callbackData: callback,
                            createdAt: now,
                        });
                    }
                }
            } else if (['airtel', 'telkom', 'faiba', 'equitel'].includes(carrierNormalized)) { // Use normalized carrier
                // --- For other carriers, only use Africa's Talking ---
                try {
                    airtimeResult = await sendAfricasTalkingAirtime(topupNumber, finalAmountToDispatch, carrier); // Use finalAmountToDispatch
                    if (airtimeResult && airtimeResult.status === 'SUCCESS') {
                        finalTxStatus = 'COMPLETED';
                        finalSalesStatus = 'COMPLETED';
                        providerUsed = 'AfricasTalking';
                        logger.info(`✅ Airtime sent successfully via Africa's Talking for ${carrier} number ${checkoutRequestID}.`);
                    } else {
                        logger.error(`❌ Africa's Talking airtime send failed for ${carrier} number ${checkoutRequestID}:`, airtimeResult);
                        finalTxStatus = 'AIRTIME_FAILED_CUSTOMER_PAID'; // Specific status: Customer paid, but airtime failed
                        finalSalesStatus = 'AIRTIME_FAILED_CUSTOMER_PAID';
                        needsReconciliation = true; // Mark for reconciliation
                        await errorsCollection.doc(`AT_API_FAIL_${checkoutRequestID}`).set({
                            type: 'AIRTIME_SEND_ERROR',
                            subType: 'AFRICASTALKING_API_FAILURE',
                            error: `Africa's Talking API returned non-SUCCESS status or unsuccessful for ${carrier}: ${JSON.stringify(airtimeResult)}`,
                            transactionCode: checkoutRequestID,
                            originalAmount: originalAmount,
                            amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                            airtimeResponse: airtimeResult,
                            callbackData: callback,
                            createdAt: now,
                        });
                    }
                } catch (atError) {
                    logger.error(`❌ Exception during Africa's Talking call for ${carrier} number ${checkoutRequestID}:`, atError.message);
                    finalTxStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION'; // General runtime exception
                    finalSalesStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION';
                    needsReconciliation = true; // Mark for reconciliation
                    await errorsCollection.doc(`AT_API_EXCEPTION_${checkoutRequestID}`).set({
                        type: 'AIRTIME_SEND_ERROR',
                        subType: 'AFRICASTALKING_API_EXCEPTION',
                        error: `Exception during Africa's Talking API call for ${carrier}: ${atError.message}`,
                        transactionCode: checkoutRequestID,
                        originalAmount: originalAmount,
                        amountDispatched: finalAmountToDispatch, // Log actual amount attempted
                        stack: atError.stack,
                        callbackData: callback,
                        createdAt: now,
                    });
                }
            } else {
                logger.warn(`⚠️ Airtime top-up not supported for carrier: ${carrier}. Marking for reconciliation.`);
                airtimeResult = { error: 'Unsupported carrier for airtime top-up.' };
                finalTxStatus = 'AIRTIME_FAILED_UNSUPPORTED_CARRIER'; // Customer paid, unsupported carrier
                finalSalesStatus = 'AIRTIME_FAILED_UNSUPPORTED_CARRIER';
                needsReconciliation = true; // Mark for reconciliation

                await errorsCollection.doc(`UNSUPPORTED_CARRIER_ONLINE_${checkoutRequestID}`).set({
                    type: 'AIRTIME_SEND_ERROR',
                    subType: 'UNSUPPORTED_CARRIER',
                    error: `Airtime top-up not supported for carrier: ${carrier}.`,
                    transactionCode: checkoutRequestID,
                    callbackData: callback,
                    createdAt: now,
                });
            }

            // --- Call Analytics Server for float deduction/adjustment ---
            try {
                logger.info(`Attempting to notify Analytics Server for transaction ${checkoutRequestID} with status: ${finalSalesStatus}...`);
                const analyticsPayload = {
                    amount: finalAmountToDispatch, // This is the amount actually *attempted* to be sent
                    status: finalSalesStatus, // Crucially, send the actual outcome status (COMPLETED or FAILED_AIRTIME_SEND)
                    telco: carrier,
                    transactionId: checkoutRequestID, // Use CheckoutRequestID as the transaction ID for the analytics server
                    mpesaReceiptNumber: mpesaReceiptNumber,
                    originalAmountPaid: originalAmount, // The amount customer paid (before bonus)
                    bonusAmount: bonusAmount, // The bonus amount applied
                    commissionRate: commissionRate, // The percentage commission rate used
                    providerUsed: providerUsed // Send which provider succeeded (or was attempted if failed)
                };
                const analyticsResponse = await axios.post(`${ANALYTICS_SERVER_URL}/api/process-airtime-purchase`, analyticsPayload);
                logger.info(`✅ Analytics Server response for float adjustment:`, analyticsResponse.data);

                // --- Log bonus to bonus_history collection ONLY if COMPLETED AND bonus was applied ---
                if (finalSalesStatus === 'COMPLETED' && bonusAmount > 0) { // Check bonusAmount > 0 to ensure bonus was calculated
                    await bonusHistoryCollection.add({
                        transactionId: checkoutRequestID,
                        type: 'STK_PUSH_BONUS',
                        carrier: carrier,
                        originalAmount: originalAmount,
                        bonusAmount: bonusAmount,
                        commissionRate: commissionRate,
                        createdAt: now,
                    });
                    logger.info(`🎁 Bonus of ${bonusAmount} for ${checkoutRequestID} logged.`);
                } else if (finalSalesStatus === 'COMPLETED' && bonusAmount === 0) {
                    logger.info(`Transaction ${checkoutRequestID} completed, but no bonus was applied or was zero.`);
                }

            } catch (deductionError) {
                logger.error(`❌ Failed to call Analytics Server for float adjustment or bonus logging:`, deductionError.message);
                await errorsCollection.doc(`ANALYTICS_FLOAT_ADJUST_FAIL_${checkoutRequestID}`).set({
                    type: 'FLOAT_ADJUSTMENT_API_ERROR',
                    error: `Failed to communicate with Analytics Server for float adjustment: ${deductionError.message}`,
                    transactionId: checkoutRequestID,
                    carrier: carrier,
                    amount: finalAmountToDispatch, // Amount that was attempted to be dispatched
                    statusReported: finalSalesStatus,
                    stack: deductionError.stack,
                    createdAt: now,
                });
                // Even if Analytics Server call fails, we proceed with local updates and reconciliation flag
            }

        } catch (err) {
            logger.error('❌ Airtime send failed (exception caught during fulfillment outside specific API calls):', err.message);
            finalTxStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION'; // More specific status
            finalSalesStatus = 'AIRTIME_FAILED_RUNTIME_EXCEPTION';
            needsReconciliation = true; // Mark for reconciliation
            await errorsCollection.doc(`AIRTIME_EXCEPTION_ONLINE_${checkoutRequestID}`).set({
                type: 'AIRTIME_SEND_ERROR',
                subType: 'RUNTIME_EXCEPTION_GENERAL',
                error: err.message,
                stack: err.stack,
                transactionCode: checkoutRequestID,
                callbackData: callback,
                createdAt: now,
            });
        }
    } else {
        // M-Pesa payment failed or was cancelled by user
        logger.info(`❌ Payment failed for ${checkoutRequestID}. ResultCode: ${resultCode}, Desc: ${callback.Body.stkCallback.ResultDesc}`);
        finalTxStatus = 'MPESA_PAYMENT_FAILED'; // M-Pesa payment itself failed
        finalSalesStatus = 'MPESA_PAYMENT_FAILED';
        // No reconciliation needed in this case as customer didn't pay successfully

        await errorsCollection.doc(`STK_PAYMENT_FAILED_${checkoutRequestID}`).set({
            type: 'STK_PAYMENT_ERROR',
            error: `STK Payment failed or was cancelled. ResultCode: ${resultCode}, ResultDesc: ${callback.Body.stkCallback.ResultDesc}`,
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });
    }

    // --- Final Updates to Firestore ---
    // 1. Update 'transactions' collection (using the checkoutRequestID as doc ID)
    await transactionsCollection.doc(checkoutRequestID).update({
        status: finalTxStatus,
        lastUpdated: now,
        mpesaReceiptNumber: mpesaReceiptNumber,
        transactionDateFromMpesa: transactionDateFromMpesa,
        phoneNumberUsedForPayment: phoneNumberUsedForPayment,
        reconciliationNeeded: needsReconciliation,
        providerUsed: providerUsed, // Which provider actually fulfilled
        secondaryProviderAttempted: secondaryProviderAttempted, // Was fallback attempted?
    });
    logger.info(`✅ [transactions] Final status for ${checkoutRequestID} updated to: ${finalTxStatus}`);

    // 2. Update 'sales' collection
    await salesCollection.doc(checkoutRequestID).update({
        status: finalSalesStatus,
        airtimeResult: airtimeResult,
        completedAt: now,
        lastUpdated: now,
        bonus: bonusAmount,
        commission_rate: commissionRate,
        total_sent: finalAmountToDispatch, // This is the amount actually attempted to be sent to the telco
        mpesaReceiptNumber: mpesaReceiptNumber,
        balanceAfterPayment: callback.Body.stkCallback.CallbackMetadata?.Item.find(item => item.Name === 'Balance')?.Value || null,
        transactionDateFromMpesa: transactionDateFromMpesa,
        phoneNumberUsedForPayment: phoneNumberUsedForPayment,
        mpesaResultCode: resultCode,
        mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
        amountPaidByCustomer: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
        errorDetails: (finalSalesStatus.includes('FAILED') && airtimeResult && airtimeResult.error) ? airtimeResult.error : null,
        fullStkCallback: callback,
        reconciliationNeeded: needsReconciliation, // Also add to sales for detailed reports
        providerUsed: providerUsed, // Which provider actually fulfilled
        secondaryProviderAttempted: secondaryProviderAttempted, // Was fallback attempted?
    });
    logger.info(`✅ [sales] Final status for ${checkoutRequestID} updated to: ${finalSalesStatus}`);

    // 3. Log to failed_reconciliationsCollection if reconciliation is needed
    if (needsReconciliation) {
        await failedReconciliationsCollection.doc(checkoutRequestID).set({
            checkoutRequestID: checkoutRequestID,
            mpesaReceiptNumber: mpesaReceiptNumber,
            originalAmountPaid: originalAmount, // The amount customer paid
            topupNumber: topupNumber,
            carrier: carrier,
            failureReason: finalSalesStatus, // Specific reason for airtime failure
            errorDetails: airtimeResult ? airtimeResult.error : 'Unknown airtime dispatch error',
            mpesaCallbackDetails: callback, // Full M-Pesa callback for context
            airtimeAttemptResult: airtimeResult, // Result from the airtime API call
            amountDispatched: finalAmountToDispatch, // Log the amount that was attempted to be dispatched
            providerUsed: providerUsed, // Could be null if both failed, or the one attempted
            secondaryProviderAttempted: secondaryProviderAttempted,
            timestamp: now,
            status: 'PENDING_REFUND_OR_MANUAL_TOPUP', // Status within reconciliation collection
            processedByAnalytics: false, // Flag for analytics server to pick up
        });
        logger.info(`⚠️ Transaction ${checkoutRequestID} marked for reconciliation in failed_reconciliations.`);
    }

    res.json({ resultCode: 0, resultDesc: 'Callback received and processed by DaimaPay server.' });
});

// --- Health Check / Root Endpoint ---
app.get('/', (req, res) => {
    res.status(200).send('DaimaPay STK Push and Airtime Dispatch Service is running!');
});

// --- Server Start ---
app.listen(PORT, () => {
    logger.info(`DaimaPay STK Push Server running on port ${PORT}`);
    logger.info(`Callback URL: ${CALLBACK_URL}`);
    logger.info(`Analytics Server URL: ${ANALYTICS_SERVER_URL}`);
    logger.info(`NODE_ENV is: ${process.env.NODE_ENV || 'development'}`);
});