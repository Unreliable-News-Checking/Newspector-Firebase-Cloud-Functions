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

export const scheduledFunction = functions.pubsub.schedule('every 30 minutes').onRun(async (context) => {

    const newsGroupCollectionRef = admin.firestore().collection('news_groups');

    const date: Date = new Date();

    const hours: number = 1

    const threshold = date.getTime() - hours * 60 * 60 * 1000;

    console.log("Threshold in milli seconds: " + threshold);

    const category_all_accounts: Map<string, Map<string, number>> = new Map();
    const tag_all_accounts: Map<string, Map<string, number>> = new Map();
    const membership_accounts: Map<string, number> = new Map();
    const tags = ["first_reporter", "close_second", "late_comer", "slow_poke", "follow_up"];

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

                    if (membership_accounts.has(key)) {
                        membership_accounts.set(key, membership_accounts.get(key)! + 1);
                    }
                    else {
                        membership_accounts.set(key, 1);
                    }

                    let account: Map<string, number> = new Map();
                    if (category_all_accounts.has(key)) {
                        account = category_all_accounts.get(key)!;
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
                    category_all_accounts.set(key, account);
                }

                // Form Map of Maps which have all tag counts for all accounts
                // Can turn into a map and then use keys as tags
                const fr_map = newsGroupDoc.get("first_reporter_map") ? newsGroupDoc.get("first_reporter_map") : {};
                const cs_map = newsGroupDoc.get("close_second_map") ? newsGroupDoc.get("close_second_map") : {};
                const lc_map = newsGroupDoc.get("late_comer_map") ? newsGroupDoc.get("late_comer_map") : {};
                const sp_map = newsGroupDoc.get("slow_poke_map") ? newsGroupDoc.get("slow_poke_map") : {};
                const fu_map = newsGroupDoc.get("follow_up_map") ? newsGroupDoc.get("follow_up_map") : {};
                const tag_map_arr = [fr_map, cs_map, lc_map, sp_map, fu_map];

                let index = 0;
                tag_map_arr.forEach(function (tagMap) {
                    for (let key in tagMap) {
                        let account: Map<string, number> = new Map();
                        if (tag_all_accounts.has(key)) {
                            account = tag_all_accounts.get(key)!;
                            if (account.has(tags[index])) {
                                account.set(tags[index], account.get(tags[index]) + tagMap[key]);
                            }
                            else {
                                account.set(tags[index], tagMap[key]);
                            }
                        }
                        else {
                            account.set(tags[index], tagMap[key]);
                        }
                        tag_all_accounts.set(key, account);
                    }
                    index++;
                });

                await newsGroupDoc.ref.update({ is_active: false }).catch(err => {
                    console.log('Error updating newsgroup status', err);
                });;
            });
        })
        .catch(err => {
            console.log('Error getting documents', err);
        });

    // Create a batch for category update
    let batch = admin.firestore().batch();

    for (let [key, value] of category_all_accounts) {
        console.log("Account: " + key);
        const accountRef = admin.firestore().collection('accounts').doc(key);
        const doc = await accountRef.get();
        const account_map: Map<string, number> = value;

        console.log("Category update for: " + key);
        let map = doc.get("category_map") ? doc.get("category_map") : {};
        let merge: boolean = false;
        let total = 0;

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

            total += count;
        }

        const membership_count = doc.get("news_group_membership_count") + membership_accounts.get(key);
        const news_count = doc.get("news_count") + total;

        let data_to_update: any = {
            category_map: map,
            news_group_membership_count: membership_count,
            news_count: news_count
        };

        console.log("Tag update for: " + key);
        const tag_map: Map<string, number> = tag_all_accounts.get(key)!;

        for (let [tag, count] of tag_map) {
            let tag_count = doc.get(tag) ? doc.get(tag) : 0;
            console.log("Tag: " + tag + " in account: " + key);

            tag_count += count;

            data_to_update[tag] = tag_count;
        }

        //update category map of account 
        if (merge === true) {
            console.log("Merge true");
            batch.set(accountRef, data_to_update, { merge: true });
        }
        else {
            console.log("Merge false");
            batch.update(accountRef, data_to_update);
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

                const account = data_after.name !== "" ? data_after.name : data_after.username;

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
            const news_source_id = data.news_source_id;
            const change = data.vote;
            var child = change === true ? "likes" : "dislikes";

            var accountRef = admin.database().ref('accounts/' + news_source_id + "/" + child);
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
    .document('user_reports_tweet/{report_id}')
    .onCreate(async (snapshot, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateReportsEvents");
        if (isProcessed === true) {
            return null;
        }

        const data = snapshot.data();

        if (data) {
            const news_article_id = data.news_article_id;
            const news_source_id = data.news_source_id;

            var accountRef = admin.database().ref('accounts/' + news_source_id + "/reports");
            accountRef.transaction(function (count) {
                return count + 1;
            }).catch(err => {
                console.log('Account reports update failure:', err);
            });

            var tweetRef = admin.database().ref('tweets/' + news_article_id + "/reports");
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


