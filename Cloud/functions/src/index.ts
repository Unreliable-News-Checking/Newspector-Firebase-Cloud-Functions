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

    const hours: number = 48

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

export const updateNewsGroupCategory = functions.firestore
    .document('news_groups/{newsgroup_id}')
    .onUpdate(async (change, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateNewsGroupCategoryEvents");
        if (isProcessed === true) {
            return null;
        }


        const data_before = change.before.data();
        const data_after = change.after.data();


        if (data_before && data_after) {

            if (data_before.category !== data_after.category) {

                //if the changed field is category do nothing
                //this block a chain trigger resulting from updating the category after a new tweet changes the dominant category type
                //the code for this operation given below
                await markEventProcessed(eventId, "updateNewsGroupCategoryEvents", Date.now());
                console.log("Category Changes (Blocking Chain Trigger...)");
                return null;
            }
            else if (data_after.is_active !== data_before.is_active) {

                await markEventProcessed(eventId, "updateNewsGroupCategoryEvents", Date.now());
                console.log("Status Changes (Blocking Chain Trigger...)");
                return null;
            }
            else {
                //when a new tweet is assigned a news_group_id the category counts get updated
                //this section of the trigger calculates the new dominant category
                //and updates the newsgroup document

                await admin.firestore().runTransaction(async t => {
                    return t.get(change.after.ref)
                        .then(doc => {

                            //Find and Update Category of Newsgroup
                            const new_dominant_category = findAndUpdateCategoryOfNewsGroup(doc, change.after.ref, t);

                            //Update the tweets perceived category
                            updatePerceivedCategoryOfTweets(change.after.id, new_dominant_category);

                        }).catch(err => {
                            console.log('Update failure:', err);
                        });

                }).catch(err => {
                    console.log('Transaction failure:', err);
                });

                await markEventProcessed(eventId, "updateNewsGroupCategoryEvents", Date.now());
                console.log("Perceived category assignment");
                return null;
            }
        }
        await markEventProcessed(eventId, "updateNewsGroupCategoryEvents", Date.now());
        console.log("No data");
        return null;
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
            if (data_before.report_count !== data_after.report_count) {
                // If the change is in reports do nothing
                // This blocks a chain trigger that results after creating a report about a tweet
                await markEventProcessed(eventId, "updateAccountInfoAfterNLPEvents", Date.now());
                console.log("Report Count changes Blocking Chain Trigger...");
                return null;
            }
            else if (data_before.perceived_category !== data_after.perceived_category && data_before.news_group_id !== "") {
                //do nothing
                await markEventProcessed(eventId, "updateAccountInfoAfterNLPEvents", Date.now());
                console.log("Perceived category changes Blocking Chain Trigger...");
                return null;
            }
            else if (data_before.news_group_id !== data_after.news_group_id) {
                // if it is the first time tweet's news_group_id set (new tweet) 
                // update the category count in the account document 
                // send topic message to users following the news_group_id
                // update the category count in the news group document

                const account = data_after.username;
                const accountRef = admin.firestore().collection('accounts').doc(account);

                const news_group_id = data_after.news_group_id;
                const newsGroupRef = admin.firestore().collection('news_groups').doc(news_group_id);

                //if this value is greater than the last update date of newsgroup it will be assigned as teh new update date
                const tweet_date = data_after.date;
                let merge_source_count_map = false;

                // Get the photos(urls) from the tweet data
                const photos = data_after.photos;
                let photo_url = ""

                //Get photo url if present
                if (photos.length > 0) {
                    photo_url = photos[0];
                }

                //Send topic message to all users following the newsgroup
                await sendTopicMessage(photo_url, account, data_after.text, data_after.news_group_id);

                await admin.firestore().runTransaction(async t => {
                    return t.getAll(newsGroupRef, accountRef)
                        .then(docs => {
                            const newsGroupDoc = docs[0]; //newsgroup document for tweet
                            const accountDoc = docs[1]; //account document for tweet
                            const accountID = data_after.username; //id of account, used for map updates
                            const categoryOfTweet = data_after.category; //category of the tweet received
                            const news_doc_id = data_after.id;
                            let last_update_date = newsGroupDoc.get("updated_at"); // the date for the last update of newsgroup
                            let newsCount = newsGroupDoc.get("count");
                            let newsTag = "slow_poke";

                            if (tweet_date > last_update_date) {
                                last_update_date = tweet_date;
                            }

                            //Source_count_map
                            let source_count_map = newsGroupDoc.get("source_count_map") ? newsGroupDoc.get("source_count_map") : {};

                            //changeValue for membership count
                            let changeValue = 0

                            //If the field exists changeValue is 0 and field value increases by 1
                            //if the field is missing changeValue is 1 and field values is set to 1
                            if (accountID in source_count_map) {
                                source_count_map[accountID] = source_count_map[accountID] + 1;
                                newsTag = "follow_up";
                            }
                            else if (!(accountID in source_count_map)) {
                                source_count_map[accountID] = 1;
                                merge_source_count_map = true;
                                changeValue = 1;

                                if (newsCount === 0) {
                                    newsTag = "first_comer";
                                } else if (newsCount === 1) {
                                    newsTag = "close_second";
                                } else if (newsCount === 2) {
                                    newsTag = "late_comer";
                                }
                            }

                            const tagCount = accountDoc.get(newsTag);

                            //update newscount and newsgroupmembership count of the account
                            t.update(accountRef, { news_group_membership_count: accountDoc.get('news_group_membership_count') + changeValue, news_count: accountDoc.get('news_count') + 1, [newsTag]: tagCount + 1 });

                            //update the category count for the newsgroup that this tweet belongs to
                            updateNewsGroup(newsGroupDoc, categoryOfTweet, newsGroupRef, t, merge_source_count_map, source_count_map, newsTag, news_doc_id, last_update_date);

                        }).catch(err => {
                            console.log('Update failure:', err);
                        });
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });

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

export const increaseFirstNewsScore = functions.firestore
    .document('news_groups/{newsgroup_id}')
    .onCreate(async (snapshot, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "increaseFirstNewsScoreEvents");
        if (isProcessed === true) {
            return null;
        }

        const data = snapshot.data();

        if (data) {
            const group_leader = data.group_leader;
            const accountRef = admin.firestore().collection('accounts').doc(group_leader);
            await admin.firestore().runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.news_group_leadership_count + 1;
                            t.update(accountRef, { news_group_leadership_count: newNumber });
                        }

                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
            await markEventProcessed(eventId, "increaseFirstNewsScoreEvents", Date.now());
            console.log("Newsgroup Exists");
            return null;
        }
        await markEventProcessed(eventId, "increaseFirstNewsScoreEvents", Date.now());
        console.log("News group does not exist");
        return null;
    });

export const updateAccountVotes = functions.firestore
    .document('votes/{vote_id}')
    .onCreate(async (snapshot, context) => {
        const eventId = context.eventId;

        const isProcessed = await isEventProcessed(eventId, "updateAccountVotesEvents");
        if (isProcessed === true) {
            return null;
        }

        const data = snapshot.data();

        if (data) {
            const accountId = data.get("account_id");
            const change = data.get("like");
            var child = change === 1 ? "likes" : "dislikes";

            var accountRef = admin.database().ref('accounts/' + accountId + "/ " + child);
            accountRef.transaction(function (currentLikes) {
                return currentLikes + change;
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

function updatePerceivedCategoryOfTweets(news_group_id: string, new_dominant_category: string) {
    const tweetsRef = admin.firestore().collection('tweets');

    tweetsRef.where('news_group_id', '==', news_group_id).get()
        .then(snapshot => {
            if (snapshot.empty) {
                console.log('No matching documents.');
                return null;
            }

            snapshot.forEach(async tweet_doc => {
                await admin.firestore().runTransaction(transaction => {
                    return transaction.get(tweet_doc.ref)
                        .then(document => {
                            const data = document.data();
                            if (data) {
                                const old_category = data.perceived_category;
                                if (old_category !== new_dominant_category) {
                                    transaction.update(tweet_doc.ref, { perceived_category: new_dominant_category });
                                }
                            }
                        }).catch(err => {
                            console.log('Update failure:', err);
                        });
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });
            });
            return null;
        })
        .catch(err => {
            console.log('Error getting documents', err);
        });
    return null;
}

function findAndUpdateCategoryOfNewsGroup(doc: FirebaseFirestore.DocumentData, newsGroupRef: FirebaseFirestore.DocumentReference, t: FirebaseFirestore.Transaction) {
    const news_group_data = doc.data();
    if (news_group_data) {
        let map = doc.get("category_map") ? doc.get("category_map") : {};
        const old_dominant_category = news_group_data.category;
        let new_dominant_category = old_dominant_category;
        let max_category_count = 0;

        if (old_dominant_category in map && old_dominant_category !== "-") {
            max_category_count = map[old_dominant_category];
        }

        for (let key in map) {
            let count = map[key];
            if (count && count > max_category_count && key !== "-") {
                new_dominant_category = key;
                max_category_count = count;
            }
        }

        //if the perceived category has a new value 
        if (old_dominant_category !== new_dominant_category) {
            //assign it to newsgroup 
            t.update(newsGroupRef, { category: new_dominant_category });
        }
        return new_dominant_category;
    }
    return null;
}

function updateNewsGroup(newsGroupDoc: FirebaseFirestore.DocumentData, categoryOfTweet: string, newsGroupRef: FirebaseFirestore.DocumentReference, t: FirebaseFirestore.Transaction, merge_source_count_map: boolean, source_count_map: Object, newsTag: string, newsId: string, updated_at: number) {
    let map = newsGroupDoc.get("category_map") ? newsGroupDoc.get("category_map") : {};
    const count = newsGroupDoc.get("count");
    if (categoryOfTweet in map) {
        map[categoryOfTweet] = map[categoryOfTweet] + 1;

        if (merge_source_count_map) {
            t.set(newsGroupRef, { category_map: map, source_count_map: source_count_map, [newsTag]: newsId, updated_at: updated_at, count: count + 1 }, { merge: true });
        }
        else {
            t.update(newsGroupRef, { category_map: map, source_count_map: source_count_map, [newsTag]: newsId, updated_at: updated_at, count: count + 1 });
        }
    }
    else if (!(categoryOfTweet in map)) {
        map[categoryOfTweet] = 1;

        t.set(newsGroupRef, { category_map: map, source_count_map: source_count_map, [newsTag]: newsId, updated_at: updated_at, count: count + 1 }, { merge: true });
    }
    else {
        console.log("Problem during update of the newsgroup category maps");
    }
}

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


