import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

interface Dic {
    [key: string]: number
}

let categories: string[] = ['Sports', 'Politics', 'Health', 'War'];

export const increaseFirstNewsScore = functions.firestore
    .document('newsgroups/{newsgroup_id}')
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
            const old_dominant_category = data_before.category;
            let new_dominant_category = "";
            let max_category_count = 0;

            categories.forEach(element => {
                let count = data_after[element]
                if (count && count > max_category_count) {
                    new_dominant_category = element;
                    max_category_count = count;
                }
            });

            if (old_dominant_category !== new_dominant_category) {
                //First set the category of the news group
                db.runTransaction(t => {
                    return t.get(change.after.ref)
                        .then(doc => {

                            t.update(change.after.ref, { category: new_dominant_category });

                        }).catch(err => {
                            console.log('Update failure:', err);
                        });
                }).then(result => {
                    console.log('Transaction success!');
                }).catch(err => {
                    console.log('Transaction failure:', err);
                });

                //Then set the perceived category of each tweet belonging to this newsgroup
                const tweetsRef = db.collection('tweets');

                tweetsRef.where('news_group_id', '==', change.after.id).get()
                    .then(snapshot => {
                        if (snapshot.empty) {
                            console.log('No matching documents.');
                            return;
                        }

                        snapshot.forEach(doc => {
                            db.runTransaction(t => {
                                return t.get(doc.ref)
                                    .then(document => {

                                        t.update(doc.ref, { perceived_category: new_dominant_category });

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

                                //update the category count for the account which is the owner of this tweet
                                const oldCategoryCount = accountData[old_perceived_category];
                                const newCategoryCount = accountData[new_perceived_category];

                                if (oldCategoryCount && newCategoryCount) {
                                    let newData = <Dic>{
                                        [old_perceived_category]: oldCategoryCount - 1,
                                        [new_perceived_category]: newCategoryCount + 1,
                                    };
                                    t.update(accountRef, newData);
                                }
                                else if (oldCategoryCount && !newCategoryCount) {
                                    let newData = <Dic>{
                                        [old_perceived_category]: oldCategoryCount - 1,
                                        [new_perceived_category]: 1,
                                    };
                                    t.set(accountRef, newData, { merge: true });
                                } else {
                                    console.log('There is a mistake resulting by previous updates, Check other triggers!');
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
                // if it is the first time tweet category and news_group_id set (new tweet) 
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


                                //update the category count for the account which is the owner of this tweet
                                const categoryCount = accountData[categoryOfTweet];

                                if (categoryCount) {
                                    let newData = <Dic>{
                                        [categoryOfTweet]: categoryCount + 1,
                                        news_count: accountData.news_count + 1
                                    };
                                    t.update(accountRef, newData);
                                }
                                else {
                                    let newData = <Dic>{
                                        [categoryOfTweet]: 1,
                                        news_count: accountData.news_count + 1
                                    };
                                    t.set(accountRef, newData, { merge: true });
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

                                //update the category count for the newsgroup that this tweet belongs to
                                const categoryCount = newsGroupData[categoryOfTweet];

                                if (categoryCount) {
                                    let newData = <Dic>{
                                        [categoryOfTweet]: categoryCount + 1,
                                    };
                                    t.update(newsGroupRef, newData);
                                }
                                else {
                                    let newData = <Dic>{
                                        [categoryOfTweet]: 1,
                                    };
                                    t.set(newsGroupRef, newData, { merge: true });
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
        }
    });