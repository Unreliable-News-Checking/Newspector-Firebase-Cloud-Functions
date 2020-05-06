import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

export const scheduledFunction = functions.pubsub.schedule('every 24 hours').onRun((context) => {
    const db = admin.firestore();

    const newsGroupCollectionRef = db.collection('tweets');

    const date: Date = new Date();

    const threshold = date.getTime() - 49 * 60 * 60 * 1000;

    newsGroupCollectionRef.where('is_active', '==', true).where('date', '<=', threshold).get()
        .then(snapshot => {
            if (snapshot.empty) {
                console.log('No matching documents.');
                return;
            }

            snapshot.forEach(newsGroupDoc => {

                newsGroupDoc.ref.update({ is_active: false }).catch(err => {
                    console.log('Error updating newsgroup status', err);
                });;

            });

        })
        .catch(err => {
            console.log('Error getting documents', err);
        });

});


export const updateNewsGroupCategory = functions.firestore
    .document('news_groups/{newsgroup_id}')
    .onUpdate((change, context) => {
        const data_before = change.before.data();
        const data_after = change.after.data();
        const db = admin.firestore();

        if (data_before && data_after) {

            if (data_before.category !== data_after.category) {

                //if the changed field is category do nothing
                //this block a chain trigger resulting from updating the category after a new tweet changes the dominant category type
                //the code for this operation given below
                console.log("Category Changes")
                return;
            }
            else if (data_after.status !== data_before.status) {

                //current category of newsgroup
                let perceived_category = data_after.category;

                //current category map of newsgroup
                let map = data_after.source_count_map ? data_after.source_count_map : {};

                //For each account in newsgroup update its category map
                for (let key in map) {
                    let accountRef = db.collection('accounts').doc(key);
                    let change_rate = map[key];
                    db.runTransaction(async t => {
                        return t.get(accountRef).then(doc => {

                            //update category map of account when the newsgroup is closed
                            updateCategoryMapOfAccount(doc, accountRef, perceived_category, change_rate, t);

                        }).catch(err => {
                            console.log('Update failure:', err);
                        });

                    }).then(result => {
                        console.log('Transaction success!');
                    }).catch(err => {
                        console.log('Transaction failure:', err);
                    });
                }
                console.log("Status Changes");
                return;
            }
            else {
                //when a new tweet is assigned a news_group_id the category counts get updated
                //this section of the trigger calculates the new dominant category
                //and updates the newsgroup document

                db.runTransaction(async t => {
                    return t.get(change.after.ref)
                        .then(doc => {

                            //Find and Update Category of Newsgroup
                            const new_dominant_category = findAndUpdateCategoryOfNewsGroup(doc, change.after.ref, t);

                            //Update the tweets perceived category
                            updatePerceivedCategoryOfTweets(db, change.after.id, new_dominant_category);

                        }).catch(err => {
                            console.log('Update failure:', err);
                        });

                }).then(result => {
                    console.log('Transaction success!');
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });

                console.log("Perceived category assignment");
                return;
            }
        }
    });


export const updateAccountInfoAfterNLP = functions.firestore
    .document('tweets/{tweet_id}')
    .onUpdate(async (change, context) => {
        const data_after = change.after.data();
        const data_before = change.before.data();
        const db = admin.firestore();

        if (data_after && data_before) {
            if (data_before.report_count !== data_after.report_count) {
                // If the change is in reports do nothing
                // This blocks a chain trigger that results after creating a report about a tweet
                return;
            }
            else if (data_before.perceived_category !== data_after.perceived_category && data_before.news_group_id !== "") {
                //do nothing
                return;
            }
            else if (data_before.news_group_id !== data_after.news_group_id) {
                // if it is the first time tweet's news_group_id set (new tweet) 
                // update the category count in the account document 
                // send topic message to users following the news_group_id
                // update the category count in the news group document

                const account = data_after.username;
                const accountRef = db.collection('accounts').doc(account);

                const news_group_id = data_after.news_group_id;
                const newsGroupRef = db.collection('news_groups').doc(news_group_id);

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
                sendTopicMessage(photo_url, account, data_after.text, data_after.news_group_id);

                db.runTransaction(async t => {
                    return t.getAll(newsGroupRef, accountRef)
                        .then(docs => {
                            const newsGroupDoc = docs[0]; //newsgroup document for tweet
                            const accountDoc = docs[1]; //account document for tweet
                            const accountID = data_after.username; //id of account, used for map updates
                            const categoryOfTweet = data_after.category; //category of the tweet received
                            let last_update_date = newsGroupDoc.get("updated_at"); // the date for the last update of newsgroup

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
                            }
                            else if (!(accountID in source_count_map)) {
                                source_count_map[accountID] = 1;
                                merge_source_count_map = true;
                                changeValue = 1;
                            }

                            //update newscount and newsgroupmembership count of the account
                            t.update(accountRef, { news_group_membership_count: accountDoc.get('news_group_membership_count') + changeValue, news_count: accountDoc.get('news_count') + 1 });

                            //update the category count for the newsgroup that this tweet belongs to
                            updateCategoryMapAndDateForNewsGroup(newsGroupDoc, categoryOfTweet, newsGroupRef, t, merge_source_count_map, source_count_map, last_update_date);

                        }).then(result => {
                            console.log("Transaction Success!");
                        }).catch(err => {
                            console.log('Update failure:', err);
                        });

                }).then(result => {
                    console.log('Transaction success!');
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });
            }
            else {
                console.log("Chain Trigger Execution Blocked");
                return;
            }
        }
    });

function updatePerceivedCategoryOfTweets(db: FirebaseFirestore.Firestore, news_group_id: string, new_dominant_category: string) {
    const tweetsRef = db.collection('tweets');

    tweetsRef.where('news_group_id', '==', news_group_id).get()
        .then(snapshot => {
            if (snapshot.empty) {
                console.log('No matching documents.');
                return;
            }

            snapshot.forEach(tweet_doc => {
                db.runTransaction(transaction => {
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
                }).then(result => {
                    console.log('Transaction success!');
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });
            });
        })
        .catch(err => {
            console.log('Error getting documents', err);
        });
}

function updateCategoryMapOfAccount(doc: FirebaseFirestore.DocumentData, accountRef: FirebaseFirestore.DocumentReference, perceived_category: string, change_rate: number, t: FirebaseFirestore.Transaction) {
    let account_map = doc.get("category_map") ? doc.get("category_map") : {};
    if (perceived_category in account_map) {
        account_map[perceived_category] += change_rate;
        t.update(accountRef, { category_map: account_map });
    }
    else {
        account_map[perceived_category] = change_rate;
        t.set(accountRef, { category_map: account_map }, { merge: true });
    }
}

function findAndUpdateCategoryOfNewsGroup(doc: FirebaseFirestore.DocumentData, newsGroupRef: FirebaseFirestore.DocumentReference, t: FirebaseFirestore.Transaction) {
    const news_group_data = doc.data();
    if (news_group_data) {
        let map = doc.get("category_map") ? doc.get("category_map") : {};
        const old_dominant_category = news_group_data.category;
        let new_dominant_category = old_dominant_category;
        let max_category_count = 0;

        if (old_dominant_category in map) {
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

function updateCategoryMapAndDateForNewsGroup(newsGroupDoc: FirebaseFirestore.DocumentData, categoryOfTweet: string, newsGroupRef: FirebaseFirestore.DocumentReference, t: FirebaseFirestore.Transaction, merge_source_count_map: boolean, source_count_map: Object, updated_at: number) {
    let map = newsGroupDoc.get("category_map") ? newsGroupDoc.get("category_map") : {};
    if (categoryOfTweet in map) {
        map[categoryOfTweet] = map[categoryOfTweet] + 1;

        if (merge_source_count_map) {
            t.set(newsGroupRef, { category_map: map, source_count_map: source_count_map, updated_at: updated_at }, { merge: true });
        }
        else {
            t.update(newsGroupRef, { category_map: map, source_count_map: source_count_map, updated_at: updated_at });
        }
    }
    else if (!(categoryOfTweet in map)) {
        map[categoryOfTweet] = 1;

        t.set(newsGroupRef, { category_map: map, source_count_map: source_count_map, updated_at: updated_at }, { merge: true });
    }
    else {
        console.log("Problem during update of the newsgroup category maps");
    }

}

function sendTopicMessage(url: string, title: string, body: string, id: string) {
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

export const increaseDislikeCount = functions.firestore
    .document('dislikes/{dislike_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const dislikedAccount = data.account;
            const accountRef = db.collection('accounts').doc(dislikedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.dislike_count + 1;
                            t.update(accountRef, { dislike_count: newNumber });
                        }
                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

export const increaseLikeCount = functions.firestore
    .document('likes/{like_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const likedAccount = data.account;
            const accountRef = db.collection('accounts').doc(likedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.like_count + 1;
                            t.update(accountRef, { like_count: newNumber });
                        }

                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

export const increaseReportCount = functions.firestore
    .document('reports/{report_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const tweet_doc_id = data.tweet_doc_id;
            const tweetRef = db.collection('tweets').doc(tweet_doc_id);
            db.runTransaction(t => {
                return t.get(tweetRef)
                    .then(doc => {
                        const tweetData = doc.data();
                        if (tweetData) {
                            const newNumber = tweetData.report_count + 1;
                            t.update(tweetRef, { report_count: newNumber });
                        }
                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

export const increaseFirstNewsScore = functions.firestore
    .document('news_groups/{newsgroup_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const group_leader = data.group_leader;
            const accountRef = db.collection('accounts').doc(group_leader);
            db.runTransaction(t => {
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
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });