import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

interface Dic {
    [key: string]: number
}

export const increaseFirstNewsScore = functions.firestore
    .document('clusters/{cluster_id}')
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
                            const newNumber = accountData.number_of_first_news_in_group + 1;
                            t.update(accountRef, { number_of_first_news_in_group: newNumber });
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

export const increaseDisapprovalCount = functions.firestore
    .document('reports/{report_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const disapprovedAccount = data.account;
            const accountRef = db.collection('accounts').doc(disapprovedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.number_of_disapprovals + 1;
                            t.update(accountRef, { number_of_disapprovals: newNumber });
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

export const increaseApprovalCount = functions.firestore
    .document('approvals/{approval_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const approvedAccount = data.account;
            const accountRef = db.collection('accounts').doc(approvedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.number_of_approvals + 1;
                            t.update(accountRef, { number_of_approvals: newNumber });
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
                            const newNumber = tweetData.reports + 1;
                            t.update(tweetRef, { reports: newNumber });
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
            if (data_before.reports !== data_after.reports) {
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
                                    notification: {
                                        title: accountData.name,
                                        body: data_after.text,
                                    },
                                    data: {
                                        click_action: "FLUTTER_NOTIFICATION_CLICK",
                                        news_group_id: data_after.cluster_id,
                                    },
                                    android: {
                                        priority: priority
                                    },
                                    topic: data_after.cluster_id
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
                                        number_of_total_news: accountData.number_of_total_news + 1
                                    };
                                    t.update(accountRef, newData);
                                }
                                else {
                                    let newData = <Dic>{
                                        [categoryOfTweet]: 1,
                                        number_of_total_news: accountData.number_of_total_news + 1
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