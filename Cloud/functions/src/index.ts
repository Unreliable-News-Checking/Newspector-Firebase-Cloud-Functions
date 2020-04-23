import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

interface Dic {
    [key: string]: number
}

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

export const increaseDislikeCount = functions.firestore
    .document('reports/{report_id}')
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
    .document('approvals/{approval_id}')
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
            }
            else {
                const account = data_after.username;
                const categoryOfTweet = data_after.category;
                const accountRef = db.collection('accounts').doc(account);

                db.runTransaction(t => {
                    return t.get(accountRef)
                        .then(doc => {
                            const accountData = doc.data();
                            if (accountData) {
                                let priority = "high" as const;

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

                                admin.messaging().send(message)
                                    .then((response) => {
                                        console.log('Successfully sent message:', response);
                                    })
                                    .catch((error) => {
                                        console.log('Error sending message:', error);
                                    });

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
            }
        }
    });