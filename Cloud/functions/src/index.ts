import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';


"use strict";
admin.initializeApp();

const isEventProcessed = async (eventId: string, collection: string) => {
    let isProcessed = false;

    await admin.database().ref('/' + collection + '/' + eventId).once("value").then(function (snapshot) {
        if (snapshot.exists()) {
            const data = snapshot.val();
            console.log("Event exists!", data);
            isProcessed = true;
        } else {
            console.log("Event does not exist!");
        }
    }).catch(err => {
        console.log('Error getting isEventProcessed:', err);
    });

    return isProcessed;
}

const markEventProcessed = async (eventId: string, collection: string, date: number) => {
    //Mark event as processed
    await admin.database().ref(collection + '/' + eventId).set({
        date: date,
    }).catch(err => {
        console.log('Error marking event:', err);
    });

    //delete events occured 15 minutes ago or more
    admin.database().ref(collection).orderByChild('date').startAt(0).endAt(date - 900000).once('value', function (snapshot) {
        snapshot.forEach(function (data) {
            admin.database().ref(collection + '/' + data.key).remove().catch(err => {
                console.log('Error deleting event:', err);
            });;
        });
    }).catch(err => {
        console.log('Error marking event:', err);
    });
}

export const scheduledFunction = functions.pubsub.schedule('every 2 hours').onRun(async (context) => {

    const newsGroupCollectionRef = admin.firestore().collection('news_groups');

    const date: Date = new Date();

    const hours: number = 1

    const threshold = date.getTime() - hours * 60 * 60 * 1000;

    console.log("Threshold in milli seconds: " + threshold);

    const all_accounts: Map<string, Map<string, number>> = new Map();

    await newsGroupCollectionRef.where('is_active', '==', true).where('created_at', '<=', threshold).get()
        .then(async snapshot => {
            if (snapshot.empty) {
                console.log('No matching documents.');
                return;
            }

            snapshot.forEach(async newsGroupDoc => {
                console.log('Traversing Snapshots...');
                //current category of newsgroup
                const perceived_category = newsGroupDoc.get("category");

                //current category map of newsgroup
                const map = newsGroupDoc.get("source_count_map") ? newsGroupDoc.get("source_count_map") : {};

                // Form Map of Maps which have all category counts for all accounts
                for (let key in map) {
                    let account: Map<string, number> = new Map();
                    if (all_accounts.has(key)) {
                        account = all_accounts.get(key)!;
                        if (account.has(perceived_category)) {
                            account.set(perceived_category, account.get(perceived_category) + map[key]);
                        }
                        else {
                            account.set(perceived_category, map[key]);
                        }
                    }
                    else {
                        account.set(perceived_category, map[key]);
                    }
                    all_accounts.set(key, account);
                }

                await newsGroupDoc.ref.update({ is_active: false }).catch(err => {
                    console.log('Error updating newsgroup status', err);
                });;
            });
        })
        .catch(err => {
            console.log('Error getting documents', err);
        });

    let batch = admin.firestore().batch();

    for (let [key, value] of all_accounts) {
        console.log("Account: " + key);
        const accountRef = admin.firestore().collection('accounts').doc(key);
        const doc = await accountRef.get();
        let account_map: Map<string, number> = value;

        console.log("Transaction for: " + doc.get("username"));
        let map = doc.get("category_map") ? doc.get("category_map") : {};
        let merge: boolean = false;

        for (let [category, count] of account_map) {
            console.log("Category: " + category + " in account: " + key);

            //if category exist in account add new values to the existing ones
            //else we should merge a new field to the category map
            if (category in map) {
                console.log("Count: " + count);
                map[category] = map[category] + count;
            }
            else {
                map[category] = count;
                merge = true;
            }
        }

        //update category map of account 
        if (merge === true) {
            console.log("Merge true");
            batch.set(accountRef, { category_map: map }, { merge: true });
        }
        else {
            console.log("Merge false");
            batch.update(accountRef, { category_map: map });
        }

    }

    await batch.commit();
    console.log("Successfull Batch");
});

export const updateAccountInfoAfterNLP = functions.firestore
    .document('tweets/{tweet_id}')
    .onUpdate(async (change, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateAccountInfoAfterNLPEvents");
        if (isProcessed === true) {
            console.log('Should return null');
            return null;
        }

        const data_after = change.after.data();
        const data_before = change.before.data();

        if (data_after && data_before) {
            if (data_before.news_group_id !== data_after.news_group_id) {
                // if it is the first time tweet's news_group_id set (new tweet) 
                // update the category count in the account document 
                // send topic message to users following the news_group_id
                // update the category count in the news group document

                const account = data_after.username;

                // Get the photos(urls) from the tweet data
                const photos = data_after.photos;
                let photo_url = ""

                //Get photo url if present
                if (photos.length > 0) {
                    photo_url = photos[0];
                }

                //Send topic message to all users following the newsgroup
                await sendTopicMessage(photo_url, account, data_after.text, data_after.news_group_id);

                await markEventProcessed(eventId, "updateAccountInfoAfterNLPEvents", Date.now());
                console.log("Finishing update block");
                return null;
            }
            else {
                await markEventProcessed(eventId, "updateAccountInfoAfterNLPEvents", Date.now());
                console.log("Chain Trigger Execution Blocked");
                return null;
            }
        }
        return null;
    });

export const updateAccountVotes = functions.firestore
    .document('user_rates_news_source/{vote_id}')
    .onCreate(async (snapshot, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateAccountVotesEvents");
        if (isProcessed === true) {
            return null;
        }

        const data = snapshot.data();

        if (data) {
            const accountId = data.get("news_source_id");
            const change = data.get("vote");
            var child = change === true ? "likes" : "dislikes";

            var accountRef = admin.database().ref('accounts/' + accountId + "/ " + child);
            accountRef.transaction(function (currentLikes) {
                return currentLikes + 1;
            }).catch(err => {
                console.log('Transaction failure:', err);
            });

            await markEventProcessed(eventId, "updateAccountVotesEvents", Date.now());
            console.log("Account votes updated");
            return null;
        }

        await markEventProcessed(eventId, "updateAccountVotesEvents", Date.now());
        console.log("No account data to update");
        return null;
    });

export const updateReportsForNewsAndSource = functions.firestore
    .document('reports/{report_id}')
    .onCreate(async (snapshot, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateReportsEvents");
        if (isProcessed === true) {
            return null;
        }

        const data = snapshot.data();

        if (data) {
            const tweet_id = data.id;
            const accountId = data.get("account_id");

            var accountRef = admin.database().ref('accounts/' + accountId + "/reports");
            accountRef.transaction(function (count) {
                return count + 1;
            }).catch(err => {
                console.log('Account reports update failure:', err);
            });

            var tweetRef = admin.database().ref('tweets/' + tweet_id + "/reports");
            tweetRef.transaction(function (count) {
                return count + 1;
            }).catch(err => {
                console.log('Tweet reports update failure:', err);
            });

            await markEventProcessed(eventId, "updateAccountVotesEvents", Date.now());
            console.log("Reports updated");
            return null;
        }

        await markEventProcessed(eventId, "updateAccountVotesEvents", Date.now());
        console.log("No account data to update");
        return null;
    });

async function sendTopicMessage(url: string, title: string, body: string, id: string) {
    let priority = "high" as const;

    //set the message that will be sent to users following the topic
    var message = {
        topic: id,
        notification: {
            title: title,
            body: body,
            image: url
        },
        data: {
            click_action: 'FLUTTER_NOTIFICATION_CLICK',
            news_group_id: id,
            title: title,
            body: body
        },
        android: {
            priority: priority,
            notification: {
                sound: 'default',
                channelId: 'very_important',
            },
        },
    };

    //send the message to users
    admin.messaging().send(message)
        .then((response) => {
            console.log('Successfully sent message:', response);
        })
        .catch((error) => {
            console.log('Error sending message:', error);
        });

}


