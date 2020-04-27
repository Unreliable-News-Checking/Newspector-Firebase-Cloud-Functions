import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

export const increaseFirstNewsScore = functions.firestore
    .document('news_groups/{newsgroup_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const groupLeader = data.groupLeader;
            const accountRef = db.collection('accounts').doc(groupLeader);
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

                return;
            }

            //when a new tweet is assigned a news_group_id the category counts get updated
            //this section of the trigger calculates the new dominant category
            //and updates the newsgroup document

            db.runTransaction(t => {
                return t.get(change.after.ref)
                    .then(doc => {

                        //First find new percieved category of the newsgroup
                        const news_group_data = doc.data();
                        if (news_group_data) {
                            let map = doc.get("category_map") ? doc.get("category_map") : {};
                            const old_dominant_category = news_group_data.category;
                            let new_dominant_category = "";
                            let max_category_count = map[old_dominant_category];

                            for (let key in map) {
                                let count = map[key];
                                if (count && count > max_category_count) {
                                    new_dominant_category = key;
                                    max_category_count = count;
                                }
                            }

                            //if the perceived category has a new value 
                            if (old_dominant_category !== new_dominant_category) {
                                //assign it to newsgroup 
                                t.update(change.after.ref, { category: new_dominant_category });
                            }

                            //Then set the dominant category to tweets that do not belong to this category
                            const tweetsRef = db.collection('tweets');

                            tweetsRef.where('news_group_id', '==', change.after.id).get()
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

export const updateAccountInfoAfterNLP = functions.firestore
    .document('tweets/{tweet_id}')
    .onUpdate((change, context) => {
        const data_after = change.after.data();
        const data_before = change.before.data();
        const db = admin.firestore();

        if (data_after && data_before) {
            if (data_before.report_count !== data_after.report_count) {
                // If the change is in reports do nothing
                // This blocks a chain trigger that results after creating a report about a tweet
            }
            else if (data_before.perceived_category !== data_after.perceived_category && data_before.news_group_id !== "") {
                //if the category is already set and percieved category changes
                //decrement the old_category count
                //increment or set the new_category count depending on existence
                const account = data_after.username;
                const accountRef = db.collection('accounts').doc(account);

                db.runTransaction(t => {
                    return t.get(accountRef)
                        .then(doc => {
                            const accountData = doc.data();
                            const old_perceived_category = data_before.perceived_category;
                            const new_perceived_category = data_after.perceived_category;
                            if (accountData) {
                                let map = doc.get("category_map") ? doc.get("category_map") : {};

                                //update the category count for the account which is the owner of this tweet

                                if (map[old_perceived_category] && map[new_perceived_category]) {
                                    map[old_perceived_category] = map[old_perceived_category] - 1;
                                    map[new_perceived_category] = map[new_perceived_category] + 1;

                                    t.update(accountRef, { category_map: map });
                                }
                                else if (map[old_perceived_category] && !map[new_perceived_category]) {
                                    map[old_perceived_category] = map[old_perceived_category] - 1;
                                    map[new_perceived_category] = 1;

                                    t.set(accountRef, { category_map: map }, { merge: true });
                                } else {
                                    console.log('There is a mistake resulting by previous updates (old_perceived_category), Check other triggers!');
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

            }
            else if (data_before.news_group_id !== data_after.news_group_id) {
                // if it is the first time tweet's news_group_id set (new tweet) 
                // update the category count in the account document 
                // send topic message to users following the news_group_id
                // update the category count in the news group document

                const account = data_after.username;
                const news_group_id = data_after.news_group_id;
                const accountRef = db.collection('accounts').doc(account);

                db.runTransaction(t => {
                    return t.get(accountRef)
                        .then(doc => {
                            const accountData = doc.data();
                            const categoryOfTweet = data_after.category;


                            if (accountData) {
                                let priority = "high" as const;

                                //set the message that will be sent to users following the topic
                                var message = {
                                    topic: data_after.news_group_id,
                                    notification: {
                                        title: accountData.name,
                                        body: data_after.text,
                                    },
                                    data: {
                                        click_action: 'FLUTTER_NOTIFICATION_CLICK',
                                        news_group_id: data_after.news_group_id,
                                    },
                                    android: {
                                        priority: priority,
                                        notification: {
                                            sound: 'default',
                                            channel_id: 'very_important',
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


                                if (categoryOfTweet !== "-") {
                                    //update the category count for the account which is the owner of this tweet
                                    let map = doc.get("category_map") ? doc.get("category_map") : {};
                                    if (map[categoryOfTweet]) {
                                        map[categoryOfTweet] = map[categoryOfTweet] + 1;

                                        t.update(accountRef, { category_map: map, news_count: accountData.news_count + 1 });
                                    }
                                    else if (!map[categoryOfTweet]) {
                                        map[categoryOfTweet] = 1;

                                        t.set(accountRef, { category_map: map, news_count: accountData.news_count + 1 }, { merge: true });
                                    } else {
                                        console.log('There is a mistake resulting by previous updates (old_perceived_category), Check other triggers!');
                                    }
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


                const newsGroupRef = db.collection('news_groups').doc(news_group_id);
                db.runTransaction(t => {
                    return t.get(newsGroupRef)
                        .then(doc => {

                            const categoryOfTweet = data_after.category;
                            const newsGroupData = doc.data();
                            if (newsGroupData) {

                                if (categoryOfTweet !== "-") {
                                    //update the category count for the newsgroup that this tweet belongs to
                                    let map = doc.get("category_map") ? doc.get("category_map") : {};

                                    if (map[categoryOfTweet]) {
                                        map[categoryOfTweet] = map[categoryOfTweet] + 1;

                                        t.update(newsGroupRef, { category_map: map });
                                    }
                                    else if (!map[categoryOfTweet]) {
                                        map[categoryOfTweet] = 1;

                                        t.set(newsGroupRef, { category_map: map }, { merge: true });
                                    } else {
                                        console.log('There is a mistake resulting by previous updates (old_perceived_category), Check other triggers!');
                                    }
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
            }
            else {
                console.log("Chain Trigger Execution Blocked");
                return;
            }
        }
    });